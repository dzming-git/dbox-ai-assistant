# -*- coding: utf-8 -*-
"""验证「继续」意图应续写上一条反馈单，而非新建一张标题为「继续」的脏单。

此前「继续」被当成普通任务走完整建单流程，因消息里没有 #单号且 prompt 经提炼后
退化为「继续」，于是建出一张标题为「继续」的全新跟踪单，而非在上一问题的反馈单下继续。

本测试确认：
- 存在历史跟踪单时，「继续」把本次分析/解决以「自动助手」身份追加回复进该单，
  不新建跟踪单；且新任务的 track_id 持久化关联到该单；
- 无历史单可续时，新单标题退化为「上一条问题的概括 / 通用标题」，而非「继续」；
- `_find_prev_track_id` 能正确回溯上一条已建单。
"""
import os
import sys
import io
import json
import queue
import tempfile
import unittest
from unittest import mock

_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'extensions_host'))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import ai_assistant as m
import platform_client as pc


class _FakeProc:
    """极简伪进程：stdout 吐一行文本、退出码 0，使 _process 正常走完。"""
    def __init__(self):
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO('已处理完成\n'.encode('utf-8'))
        self.stderr = io.BytesIO(b'')
        self.returncode = 0
        self.pid = 1

    def poll(self):
        return 0

    def wait(self, timeout=None):
        return 0

    def kill(self):
        pass

    def communicate(self, *a, **k):
        return b'', b''


class ContinueTicketTest(unittest.TestCase):
    def test_find_prev_track_id(self):
        mgr = m.AIChatManager()
        mgr.init(tempfile.mkdtemp())
        # 无任何跟踪单
        self.assertIsNone(mgr._find_prev_track_id(None))
        # 插入一条已完成且带 track_id 的历史任务
        mgr._insert_task('ai_prev', '上一轮的问题：下载服务异常', None, m.AIChatManager.STATUS_COMPLETED)
        mgr._set_track_id('ai_prev', '202608130018')
        self.assertEqual(mgr._find_prev_track_id(None), '202608130018')
        # 非 completed 状态的 track_id 不应被回溯
        mgr._insert_task('ai_run', '进行中', None, m.AIChatManager.STATUS_RUNNING)
        mgr._set_track_id('ai_run', '202608130099')
        self.assertEqual(mgr._find_prev_track_id(None), '202608130018')

    def test_continue_appends_to_prev_ticket(self):
        """存在历史跟踪单时，「继续」应续写该单、不新建单，且 track_id 关联过去。"""
        mgr = m.AIChatManager()
        mgr.init(tempfile.mkdtemp())
        # 历史任务：已完成 + 已建跟踪单
        mgr._insert_task('ai_prev', '上一轮的问题：下载服务异常', 7, m.AIChatManager.STATUS_COMPLETED)
        mgr._set_track_id('ai_prev', '202608130018')

        comments = []
        file_calls = []

        def fake_add(issue_id, content):
            comments.append((issue_id, content))
            return True

        def fake_file_feedback(ftype, title, content, extra=None, status='open',
                               comment=None, comments=None):
            file_calls.append(title)
            return '202608139999'

        with mock.patch.object(m.subprocess, 'Popen', lambda *a, **k: _FakeProc()), \
             mock.patch.object(m, '_resolve_buddy_cli', lambda: 'codebuddy'), \
             mock.patch.object(m, '_git_rev_head', return_value='c0'), \
             mock.patch.object(m, '_git_dirty_files', return_value=set()), \
             mock.patch.object(pc, 'add_feedback_comment', fake_add), \
             mock.patch.object(pc, 'file_feedback', fake_file_feedback):
            import uuid
            tid = 'ai_' + uuid.uuid4().hex[:16]
            mgr._insert_task(tid, '继续处理这个问题', 7, m.AIChatManager.STATUS_PENDING)
            q = queue.Queue()
            with mgr._lock:
                mgr._subscribers[tid] = [q]
            mgr._process(tid)

        # 不应新建跟踪单
        self.assertEqual(file_calls, [], '继续应续写既有单，而非新建跟踪单')
        # 分析 + 解决两条留言都回复进 prev 单
        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[0][0], '202608130018')
        self.assertEqual(comments[1][0], '202608130018')
        # 新任务 track_id 持久化关联到 prev 单
        self.assertEqual(mgr.get_task(tid)['track_id'], '202608130018')
        self.assertEqual(mgr.get_task(tid)['status'], m.AIChatManager.STATUS_COMPLETED)
        # 阶段结论应表明「续写」而非「新建」
        stored = mgr.get_task(tid)['reply'] or ''
        self.assertIn('续写', stored)
        self.assertIn('#202608130018', stored)

    def test_continue_no_prev_uses_context_title(self):
        """无历史单可续时，新单标题应是「上一条问题的概括 / 通用标题」，而非「继续」。"""
        mgr = m.AIChatManager()
        mgr.init(tempfile.mkdtemp())
        # 提供一条历史对话（无 track_id，但含回复），供标题退化使用其概括
        mgr._insert_task('ai_hist', '首页下载按钮点击无响应需要修复', None, m.AIChatManager.STATUS_COMPLETED)
        mgr._set_status('ai_hist', m.AIChatManager.STATUS_COMPLETED, reply='已修复下载按钮')

        captured = {}

        def fake_file_feedback(ftype, title, content, extra=None, status='open',
                               comment=None, comments=None):
            captured['title'] = title
            return '202608130777'

        with mock.patch.object(m.subprocess, 'Popen', lambda *a, **k: _FakeProc()), \
             mock.patch.object(m, '_resolve_buddy_cli', lambda: 'codebuddy'), \
             mock.patch.object(m, '_git_rev_head', side_effect=['oldc', 'newc', 'newc']), \
             mock.patch.object(m, '_git_dirty_files', return_value=set()), \
             mock.patch.object(pc, 'file_feedback', fake_file_feedback):
            import uuid
            tid = 'ai_' + uuid.uuid4().hex[:16]
            mgr._insert_task(tid, '继续', None, m.AIChatManager.STATUS_PENDING)
            q = queue.Queue()
            with mgr._lock:
                mgr._subscribers[tid] = [q]
            mgr._process(tid)

        self.assertIn('title', captured)
        self.assertNotEqual(captured['title'], '继续')
        # 退化为上一条问题的概括
        self.assertEqual(captured['title'], '首页下载按钮点击无响应需要修复')

    def test_continue_bare_filler_title(self):
        """纯「继续」且无任何历史时，标题应为通用兜底，绝不叫「继续」。"""
        self.assertEqual(m._make_ticket_title('继续'), 'AI 处理任务')
        self.assertEqual(m._make_ticket_title('帮我'), 'AI 处理任务')

    def test_build_prompt_injects_prev_issue(self):
        """「继续」意图应把续写的反馈单号注入提示，使模型基于上一条问题推进。"""
        mgr = m.AIChatManager()
        mgr.init(tempfile.mkdtemp())
        p = mgr._build_prompt('继续处理', intent='continue', prev_issue='202608130018')
        self.assertIn('#202608130018', p)
        self.assertIn('延续', p)


if __name__ == '__main__':
    unittest.main()
