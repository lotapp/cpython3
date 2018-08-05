import os
import sys
import signal

from . import util

__all__ = ['Popen']

#
# Start child process using fork
# 3.4.x开始，在popen_fork文件中（以前是multiprocessing.forking.py）
# https://github.com/python/cpython/blob/3.7/Lib/multiprocessing/popen_fork.py
#


class Popen(object):
    method = 'fork'

    def __init__(self, process_obj):
        util._flush_std_streams()
        self.returncode = None
        self.finalizer = None
        self._launch(process_obj)

    def duplicate_for_child(self, fd):
        return fd

    # 回顾一下上次说的：os.WNOHANG - 如果没有子进程退出，则不阻塞waitpid()调用
    def poll(self, flag=os.WNOHANG):
        if self.returncode is None:
            try:
                # 他的内部调用了waitpid
                pid, sts = os.waitpid(self.pid, flag)
            except OSError as e:
                # 子进程尚未创建. See #1731717
                # e.errno == errno.ECHILD == 10
                return None
            if pid == self.pid:
                if os.WIFSIGNALED(sts):
                    self.returncode = -os.WTERMSIG(sts)
                else:
                    assert os.WIFEXITED(sts), "Status is {:n}".format(sts)
                    self.returncode = os.WEXITSTATUS(sts)
        return self.returncode

    def wait(self, timeout=None):
        # 设置超时的一系列处理
        if self.returncode is None:
            if timeout is not None:
                from multiprocessing.connection import wait
                if not wait([self.sentinel], timeout):
                    return None
            # wait()成功返回后会执行
            return self.poll(os.WNOHANG if timeout == 0.0 else 0)
        return self.returncode

    def _send_signal(self, sig):
        if self.returncode is None:
            try:
                os.kill(self.pid, sig)
            except ProcessLookupError:
                pass
            except OSError:
                if self.wait(timeout=0.1) is None:
                    raise

    def terminate(self):
        self._send_signal(signal.SIGTERM)

    def kill(self):
        self._send_signal(signal.SIGKILL)

    def _launch(self, process_obj):
        code = 1
        parent_r, child_w = os.pipe()
        self.pid = os.fork()
        if self.pid == 0:
            try:
                os.close(parent_r)
                code = process_obj._bootstrap()
            finally:
                os._exit(code)
        else:
            os.close(child_w)
            self.finalizer = util.Finalize(self, os.close, (parent_r, ))
            self.sentinel = parent_r

    def close(self):
        if self.finalizer is not None:
            self.finalizer()
