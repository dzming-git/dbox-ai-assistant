# -*- coding: utf-8 -*-
"""AI 助手「停止/取消」运行中任务时，必须杀掉整个进程树，而非仅立即进程。

回归点：此前 delete_task 对 running 任务调用 proc.kill() 只杀掉 buddy 启动进程，
而真正的 AI 工作跑在孙进程（node）里并持有 stdout 管道 —— 孙进程不死、管道不关、
_run_cli 读取循环不结束、_process 挂起、单 worker 线程卡死，表现为
「点了停止任务还在跑、后面的任务排队不动」。

本测试用伪进程验证：取消 running 任务时走的是 self._terminate（进程树级 kill），
而不是仅 proc.kill()（仅父进程）。
"""
import os
import sys
import tempfile
import unittest
from unittest import mock

_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'extensions_host'))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import ai_assistant as m


class _FakeProc:
    """伪进程：poll() 返回 None（仍存活），kill() 会被记录；用于区分
    「仅杀父进程」与「杀整个进程树」两条取消路径。"""
    def __init__(self):
        self.pid = 1234
        self.kill_called = False

    def poll(self):
        return None

    def kill(self):
        self.kill_called = True


class StopCancelTest(unittest.TestCase):
    def setUp(self):
        self.mgr = m.AIChatManager()
        self.data_dir = tempfile.mkdtemp()
        self.mgr.init(self.data_dir)
        # 阻断副作用：emit / 统一任务表 / 反馈 spool
        self.mgr._emit = lambda *a, **k: None
        self.mgr._remove_from_unified = lambda *a, **k: None

    def tearDown(self):
        import shutil
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_stop_running_kills_process_tree_not_just_parent(self):
        # 注册一个「运行中」的伪进程
        task_id = 'ai_stop_001'
        self.mgr._insert_task(task_id, '帮我改一下首页', None, m.AIChatManager.STATUS_RUNNING)
        proc = _FakeProc()
        self.mgr._procs[task_id] = proc

        terminate_calls = []

        def _fake_terminate(p):
            # 接管 _terminate：仅记录被调用，且【不】替 delete_task 调用 proc.kill()
            terminate_calls.append(p)

        with mock.patch.object(self.mgr, '_terminate', side_effect=_fake_terminate):
            result = self.mgr.delete_task(task_id)

        # 取消应成功
        self.assertTrue(result)
        # 关键：走的是 _terminate（进程树级 kill），且被调用了一次
        self.assertEqual(len(terminate_calls), 1, '取消运行中任务必须调用 _terminate 杀进程树')
        self.assertIs(terminate_calls[0], proc)
        # 回归点：delete_task 不应再直接调用 proc.kill()（那只杀父进程、杀不掉孙进程，
        # 导致管道不关、_run_cli 卡死、worker 卡死、后续任务排队不动）。
        self.assertFalse(proc.kill_called, 'delete_task 不应直接 proc.kill()（仅杀父进程）')
        # 真实运行中：进程树被杀 → _run_cli 读取循环结束 → worker 置状态为 cancelled；
        # 本单测未驱动 worker，故这里只验证取消入口正确走进程树 kill 路径。


if __name__ == '__main__':
    unittest.main(verbosity=2)
