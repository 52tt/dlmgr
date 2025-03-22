#!/usr/bin/env python3

import json
import os
import queue
import re
import shlex
import signal
import socket
import subprocess
import sys
import threading
import traceback
import time

from typing import Optional


def err(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)


def human_readable_size(size):
    '''Convert bytes to human-readable format.'''
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f'{size:.1f} {unit}'
        size /= 1024
    return f'{size:.1f} TB'


# Constants
DEBUG = True
PID_FILE = '/tmp/dlmgr.pid'
LISTEN_PATH = '/tmp/dlmgr.sock'
BANDWIDTH_LIMIT = 16 * 1024 * 1024  # 16 MB/s
MAX_RUNNING_JOBS = 4


def recv_until_eof(sock, bufsize=4096):
    data = bytearray()
    while True:
        partial = sock.recv(bufsize)
        if not partial: # EOF detected
            break
        data.extend(partial)
    return bytes(data)


def parse_output_file(argv):
    basename = os.path.basename(argv[0])
    if basename == 'curl':
        options_for_output_file = {'-o', '--output'}
    elif basename == 'aria2c':
        options_for_output_file = {'-o', '--out'}
    else:
        err(f'{basename} command not supported yet')
        return None
    for i, v in enumerate(argv):
        if i >= 1 and argv[i-1] in options_for_output_file:
            return v
    return None


def rename_output_file(cmd, new_file_name):
    argv = shlex.split(cmd)
    basename = os.path.basename(argv[0])
    if basename == 'curl':
        options_for_output_file = {'-o', '--output'}
    elif basename == 'aria2c':
        options_for_output_file = {'-o', '--out'}
    else:
        err(f'{basename} command not supported yet')
        return cmd
    for i, _ in enumerate(argv):
        if i >= 1 and argv[i-1] in options_for_output_file:
            argv[i] = new_file_name
            break
    return shlex.join(argv)

class OP(object):
    '''OP'''
    ADD_JOB = 'add_job'
    REMOVE_JOB = 'remove_job'
    LIST_JOBS = 'list_jobs'
    FORCE_RUN_NEXT_JOB = 'force_run_next_job'
    RENAME_OUTPUT_FILE = 'rename_output_file'


class RunningJob(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.argv = shlex.split(cmd)
        basename = os.path.basename(self.argv[0])
        if basename == 'curl':
            if '-v' not in self.argv:
                self.argv.insert(1, '-v')
        self.output_file = parse_output_file(self.argv)
        self.start_time = time.time()
        self.file_size = None
        self.downloaded_size = 0
        self._unconsumed_child_stderr_ouput = ''
        self.child = subprocess.Popen(self.argv, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        os.set_blocking(self.child.stderr.fileno(), False)

    def refresh_downloaded_size(self):
        self.downloaded_size = os.path.getsize(self.output_file) if os.path.exists(self.output_file) else 0

    def consume_child_stderr(self):
        def get_content_length(s):
            m = re.search(r'(?<=(Content-Length|content-length): )[0-9]+', s, re.M)
            if m and m.group():
                err('Got Content-Length', m.group())
                return int(m.group())
            return None

        if rx_bytes := self.child.stderr.read():
            try:
                rx_text = rx_bytes.decode()
            except Exception as e:
                err(f'Error decoding stderr output {rx_bytes}: {e}')
                rx_text = None

            if rx_text:
                # Find line end
                for c in ['\n', '\r']:
                    first_idx = rx_text.find(c)
                    if first_idx >= 0:
                        break

                if first_idx >= 0:  # '\n' or '\r' found
                    text = self._unconsumed_child_stderr_ouput + rx_text[:first_idx + 1]
                    if self.file_size is None:
                        self.file_size = get_content_length(text)

                    for c in ['\n', '\r']:
                        last_idx = rx_text.rfind(c)
                        if last_idx >= 0:
                            break

                    if last_idx > first_idx:
                        text = rx_text[first_idx + 1:last_idx + 1]
                        if self.file_size is None:
                            self.file_size = get_content_length(text)
                    else:  # last_idx == first_idx:
                        pass
                    self._unconsumed_child_stderr_ouput = rx_text[last_idx + 1:]
                else:
                    self._unconsumed_child_stderr_ouput += rx_text

                rx_text = None



class DownloadManager(object):
    '''Daemon mode: Manages download jobs in the background.'''
    def __init__(self):
        self._start_time = time.time()
        self._last_speed_check = self._start_time
        self._sock = None
        self._running_jobs = {}
        self._pending_jobs = []
        self._killed_jobs = set()
        self._incoming_requests = queue.Queue()
        self._stopped = False  # To notify threads to exit
        self._download_speed = 0
        self._max_running_jobs = MAX_RUNNING_JOBS
        self._bandwidth_limit = BANDWIDTH_LIMIT

    def _get_increased_size(self):
        total_increased_size = 0
        for job in self._running_jobs.values():
            prev_size = job.downloaded_size
            job.refresh_downloaded_size()
            total_increased_size += job.downloaded_size - prev_size
        return total_increased_size

    def _refresh_download_speed(self, force=False):
        now = time.time()
        duration = now - self._last_speed_check
        if force or (duration >= 5):
            self._download_speed = self._get_increased_size() / duration
            err(f'Download speed {human_readable_size(self._download_speed)}/s')
            self._last_speed_check = now

    def _enqueue_job(self, cmd):
        job_id = self._assign_job_id()
        self._pending_jobs.append((job_id, cmd))
        return job_id

    def _assign_job_id(self):
        job_id = getattr(self, '_job_id', 0)
        self._job_id = job_id + 1
        return job_id

    def _try_to_run_next_job(self) -> Optional[int]:
        if not self._pending_jobs:
            return None
        if len(self._running_jobs) >= self._max_running_jobs:
            # err(f'Already {len(self._running_jobs)} jobs running')
            return None
        if self._download_speed >= self._bandwidth_limit * 0.80:
            err(f'Bandwidth limit reached: {self._download_speed:.1f} >= {self._bandwidth_limit} * 80%')
            return None
        return self._run_next_job()

    def _run_next_job(self):
        if not self._pending_jobs:
            return None
        job_id, cmd = self._pending_jobs.pop(0)
        try:
            self._running_jobs[job_id] = RunningJob(cmd)
        except Exception:
            err(f'Got exception when starting job {job_id}')
            traceback.print_exec()
        return job_id

    def notify_main_thread(self, request):
        self._incoming_requests.put(request)

    def server_thread(self, listen_path):
        if os.path.exists(listen_path):
            os.remove(listen_path)
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(listen_path)
        self._sock.listen(1)
        self._sock.settimeout(1)
        while not self._stopped:
            try:
                conn, _ = self._sock.accept()
                request = recv_until_eof(conn, bufsize=1024).decode()
                request = json.loads(request)
                request['conn'] = conn
            except Exception:
                pass
            else:
                self.notify_main_thread(request)  # Hand over to the main thread
        os.remove(listen_path)

    def _handle_request(self, request):
        conn = request.pop('conn', None)
        err('_handle_request', request, '...')
        op = request.get('op')
        if op == OP.ADD_JOB:
            cmd = request['cmd'].replace('\\\n', '')
            job_id = self._enqueue_job(cmd)
            conn.sendall(f'Job added as {job_id}.'.encode())
        elif op == OP.REMOVE_JOB:
            job_id = request.get('job_id')
            if job := self._running_jobs.get(job_id):
                try:
                    os.kill(job.child.pid, signal.SIGTERM)
                    conn.sendall(f'Running job {job_id} killed.'.encode())
                    self._killed_jobs.add(job_id)
                except ProcessLookupError:
                    pass
            else:
                idx = next((i for i, (_job_id, _) in enumerate(self._pending_jobs) if _job_id == job_id), None)
                if idx:
                    del self._pending_jobs[idx]
                    conn.sendall(f'Pending job {job_id} removed.'.encode())
                else:
                    conn.sendall(f'Job {job_id} not found.'.encode())
        elif op == OP.LIST_JOBS:
            response = [f'{len(self._running_jobs)} running jobs:']
            for job_id in sorted(self._running_jobs.keys()):
                job: RunningJob = self._running_jobs[job_id]

                file_size_txt = 'unknown'
                downloaded_size_txt = human_readable_size(job.downloaded_size)
                if job.file_size:
                    file_size_txt = f'{human_readable_size(job.file_size)} ({job.file_size})'
                    downloaded_size_txt += f' ({100 * job.downloaded_size / job.file_size:.1f}%)'
                average_download_speed = job.downloaded_size / (time.time() - job.start_time)
                response.extend([
                    f'  Job {job_id}:',
                    f'    cmd: {job.cmd}',
                    f"    output_file: '{job.output_file}'",
                    f'    start_time: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job.start_time))}',
                    f'    file_size: {file_size_txt}',
                    f'    downloaded_size: {downloaded_size_txt}',
                    f'    average_download_speed: {human_readable_size(average_download_speed)}/s',
                ])

            response.append(f'{len(self._pending_jobs)} pending jobs:')
            for job_id, cmd in self._pending_jobs:
                response.extend([
                    f'  Job {job_id}:',
                    f'    cmd: {cmd}',
                ])
            conn.sendall('\n'.join(response).encode())
        elif op == OP.FORCE_RUN_NEXT_JOB:
            job_id = self._run_next_job()
            conn.sendall(('No pending jobs to start.' if job_id is None else \
                          (f'Job {job_id} successfully started' if job_id in self._running_jobs else \
                           f'Job {job_id} failed to start')).encode())
        elif op == OP.RENAME_OUTPUT_FILE:
            job_id = request.get('job_id')
            new_file_name = request.get('output_file')
            if job_id in self._running_jobs:
                job: RunningJob = self._running_jobs[job_id]
                old_file_name = job.output_file
                job.output_file = new_file_name
                try:
                    os.rename(old_file_name, new_file_name)
                    conn.sendall(f'Running job {job_id} output file renamed to {new_file_name}.'.encode())
                except Exception as e:
                    conn.sendall(f'Failed to rename output file: {e}'.encode())
            else:
                idx = next((i for i, (_job_id, _) in enumerate(self._pending_jobs) if _job_id == job_id), None)
                idx = None
                cmd = None
                for idx, (_job_id, cmd) in enumerate(self._pending_jobs):
                    if _job_id == job_id:
                        break
                if idx is not None:
                    cmd = rename_output_file(cmd, new_file_name)
                    self._pending_jobs[idx] = (job_id, cmd)
                    conn.sendall(f'Pending job {job_id} output file renamed to {new_file_name}.'.encode())
                else:
                    conn.sendall(f'Job {job_id} not found.'.encode())
        else:
            err(f'Unknown op: {op}')
        if conn:
            conn.shutdown(socket.SHUT_WR)  # Shut down write side
            conn.close()

    def main_loop(self):
        idle_time = 0
        while not self._stopped:
            if self._running_jobs:
                idle_time = 0  # Reset idle time

                self._refresh_download_speed()

                exited_jobs = []
                for job_id, job in self._running_jobs.items():
                    job.consume_child_stderr()

                    # Check if the job has exited
                    if (exit_code := job.child.poll()) is not None:  # Job exited
                        err(f"Job {job_id} '{job.output_file}' exited with code {exit_code}")
                        exited_jobs.append(job_id)

                # Cleanup exited jobs
                if exited_jobs:
                    self._refresh_download_speed(force=True)
                    for job_id in exited_jobs:
                        job: RunningJob = self._running_jobs.pop(job_id, None)
                        if job and job.file_size:
                            job.refresh_downloaded_size()
                            completed = (job.downloaded_size >= job.file_size)
                            err("Job {} '{}' {}completed, {:,}/{:,} downloaded".format( \
                                job_id, job.output_file, ('not ' if not completed else ''), job.downloaded_size, job.file_size))
                            if not completed:
                                # Restart the job
                                restarted_job = RunningJob(job.cmd)
                                restarted_job.file_size = job.file_size
                                restarted_job.downloaded_size = job.downloaded_size
                                restarted_job.start_time = job.start_time
                                self._running_jobs[job_id] = restarted_job
                                err(f"Job {job_id} '{job.output_file}' restarted.")
                        if job_id in self._killed_jobs:
                            try:
                                os.remove(job.output_file)
                                err(f"Job {job_id} '{job.output_file}' removed.")
                            except Exception:
                                pass
            try:
                request = self._incoming_requests.get(timeout=1)
                idle_time = 0  # Reset idle time
                self._handle_request(request)
            except queue.Empty:
                pass

            if not self._pending_jobs:
                if not self._running_jobs:
                    idle_time += 1
                    if idle_time >= 5:
                        err('Idle for 5s, exiting ...')
                        self._stopped = True
                        break
            else:
                self._try_to_run_next_job()

    def run(self):
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        thread = threading.Thread(target=self.server_thread, name='server_thread', args=(LISTEN_PATH,))
        thread.start()
        try:
            self.main_loop()
        except Exception:
            self._stopped = True
            err('Got fatal exception:')
            traceback.print_exc()
        thread.join()
        os.remove(PID_FILE)
        err('Daemon exited.')

    def sigterm_handler(self, sig, frame):
        self._stopped = True


class DownloadManagerCLI:
    '''DownloadManagerCLI'''

    @staticmethod
    def help():
        name = os.path.basename(sys.argv[0])
        print(f'''Usage:
    {name} help                                     # Show this help message
    {name} add_job <download_command>               # Add a download job
    {name} remove_job <job_id>                      # Remove a job by its ID
    {name} list_jobs                                # List all running and pending jobs
    {name} force_run_next_job                       # Force the daemon to run the next job immediately
    {name} rename_output_file <job_id> <file_name>  # Rename the output file for a job by its ID
''')

    def list_jobs(self):
        '''List all running jobs.'''
        response = self._exchange_data(json.dumps({'op': OP.LIST_JOBS}))
        print(response)

    def add_job(self, download_cmd=None):
        '''Add a download job. If no command is provided, read from stdin.'''
        if download_cmd:
            self._add_job(download_cmd)
        else:
            for line in sys.stdin:
                self._add_job(line.strip())

    def _add_job(self, download_cmd):
        response = self._exchange_data(json.dumps({'op': OP.ADD_JOB, 'cmd': download_cmd}, ensure_ascii=False))
        print(response)

    def remove_job(self, job_id: int):
        '''Remove a job by its ID.'''
        response = self._exchange_data(json.dumps({'op': OP.REMOVE_JOB, 'job_id': job_id}))
        print(response)

    def _exchange_data(self, data):
        retry_times, retry_interval = 2, 0.5
        if not self._is_daemon_running():
            self._start_daemon()
            retry_times, retry_interval = 5, 0.2
            time.sleep(0.2)  # Wait for Daemon to start
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        for retry in range(retry_times):
            try:
                sock.connect(LISTEN_PATH)
                break
            except Exception:
                if retry == retry_times - 1:
                    raise
                time.sleep(retry_interval)

        # sendall() requires bytes, not str
        if isinstance(data, str):
            data = data.encode()

        try:
            sock.sendall(data)
            sock.shutdown(socket.SHUT_WR)  # Shut down write side
            response = bytearray()
            while True:
                part = sock.recv(4096)
                if not part: # EOF detected
                    break
                response.extend(part)
            response = bytes(response).decode()
        finally:
            sock.close()
        return response

    def _is_daemon_running(self):
        try:
            daemon_pid = int(open(PID_FILE).read())
            os.kill(daemon_pid, 0)
            return True
        except PermissionError:
            return True
        except FileNotFoundError:
            return False
        except ProcessLookupError:
            return False
        except Exception:
            return False

    def force_run_next_job(self):
        '''Notify the daemon to run the next job immediately.'''
        response = self._exchange_data(json.dumps({'op': OP.FORCE_RUN_NEXT_JOB}, ensure_ascii=False))
        print(response)

    def rename_output_file(self, job_id: int, file_name: str):
        response = self._exchange_data(json.dumps({'op': OP.RENAME_OUTPUT_FILE, 'job_id': job_id, 'output_file': file_name}, ensure_ascii=False))
        print(response)

    def _start_daemon(self):
        if DEBUG:
            subprocess.run(['bash'], input=f'{sys.executable} {__file__} server &'.encode())
        else:
            subprocess.Popen([sys.executable, __file__, 'server'])


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'server':
        # Daemon mode, started by CLI
        if DEBUG:
            DownloadManager().run()
        else:
            import daemon
            with daemon.DaemonContext():
                DownloadManager().run()
    else:
        # CLI mode
        import fire
        fire.Fire(DownloadManagerCLI)

if __name__ == '__main__':
    main()
