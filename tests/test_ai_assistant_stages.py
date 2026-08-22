# -*- coding: utf-8 -*-
"""AI 助手「每个阶段一个气泡」测试。

验证：宿主进程在 _process 管线各阶段向订阅者发射 phase 事件（开始 -> 结束，结束带一句
conclusion），每个阶段对应聊天窗口里一个独立气泡；闲聊意图只产生「分析用户意图 + 生成回复」
两个气泡，不触发 git 核查与建单。

通过伪造 subprocess.Popen，使 _process 在不依赖真实 buddy CLI 的情况下
跑通全链路，并断言 phase 事件的数量、顺序与 conclusion，以及存库的 phases / reply。

运行：python tests/test_ai_assistant_stages.py
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


class IntentClassifyTest(unittest.TestCase):
    """意图判定（建议 / 缺陷 / 继续 / 闲聊）确定性测试。"""
    def test_defect(self):
        self.assertEqual(m._classify_intent('稍后再看里有个视频删了又出现，排查一下'), 'defect')
        self.assertEqual(m._classify_intent('下载服务异常，状态显示不正确'), 'defect')

    def test_suggestion(self):
        self.assertEqual(m._classify_intent('建议增加一个批量导出功能'), 'suggestion')
        self.assertEqual(m._classify_intent('希望优化一下脚本流程'), 'suggestion')

    def test_continue(self):
        self.assertEqual(m._classify_intent('继续上面的修复'), 'continue')
        self.assertEqual(m._classify_intent('刚才那个问题再处理一下'), 'continue')

    def test_chat(self):
        self.assertEqual(m._classify_intent('你好'), 'chat')
        self.assertEqual(m._classify_intent('谢谢'), 'chat')

    def test_work_category_from_intent(self):
        # 反馈中心类型应复用意图判定：缺陷 -> bug，建议 -> suggestion
        self.assertEqual(m._classify_work_category('下载服务异常，排查一下'), 'bug')
        self.assertEqual(m._classify_work_category('建议增加批量导出'), 'suggestion')
        self.assertEqual(m._classify_work_category('继续上面的修复'), 'other')


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


def _collect_phases(mgr, tid):
    """从事件队列抽取 phase 事件，按 index 合并为最终阶段列表
    [(index, label, kind, state, conclusion)]，与前端 upsert 行为一致。"""
    q = queue.Queue()
    with mgr._lock:
        mgr._subscribers[tid] = [q]
    mgr._process(tid)
    events = []
    while True:
        try:
            item = q.get(timeout=2)
        except queue.Empty:
            break
        events.append(item)
    merged = {}  # index -> (label, kind, state, conclusion)
    for k, d in events:
        if k == 'phase':
            o = json.loads(d)
            idx = o.get('index')
            cur = merged.get(idx, ['', '', 'running', ''])
            if o.get('label') is not None:
                cur[0] = o['label']
            if o.get('kind') is not None:
                cur[1] = o['kind']
            if o.get('state') is not None:
                cur[2] = o['state']
            if o.get('conclusion') is not None:
                cur[3] = o['conclusion']
            merged[idx] = cur
    phases = [(idx,) + tuple(merged[idx]) for idx in sorted(merged.keys())]
    return events, phases


class PhaseEmitTest(unittest.TestCase):
    def test_process_emits_phase_bubbles(self):
        mgr = m.AIChatManager()
        tmp = tempfile.mkdtemp()
        mgr.init(tmp)

        with mock.patch.object(m.subprocess, 'Popen', lambda *a, **k: _FakeProc()), \
             mock.patch.object(m, '_resolve_buddy_cli', lambda: 'codebuddy'):
            import uuid
            tid = 'ai_' + uuid.uuid4().hex[:16]
            mgr._insert_task(tid, '排查一下下载服务异常（分阶段测试）', None, m.AIChatManager.STATUS_PENDING)
            events, phases = _collect_phases(mgr, tid)

        labels = [p[1] for p in phases]
        self.assertTrue(labels, '应至少发射一个 phase 事件')
        # 阶段应按处理顺序出现：首项为「分析用户意图」、末项为「收尾核查」之类，
        # 且覆盖做事前核查、AI 分析定位、AI 执行处理等关键阶段。
        self.assertIn('分析用户意图（建议 / 缺陷 / 继续 / 闲聊）', labels)
        self.assertTrue(any('git 仓库状态' in (s or '') for s in labels), '应含做事前/后核查阶段')
        self.assertTrue(any('AI 分析定位问题' in (s or '') for s in labels), '应含 AI 分析定位阶段')
        self.assertTrue(any('AI 执行处理' in (s or '') for s in labels), '应含 AI 执行处理阶段')
        # 首个阶段为宿主判定意图，且以一句结论收尾（分析用户意图：这是一条【缺陷】反馈）
        first = phases[0]
        self.assertEqual(first[2], 'host')
        self.assertIn('分析用户意图：这是一条', first[4])
        # 每个阶段开始（state=running）与结束（state=done）成对出现，最终均 done
        self.assertTrue(all(p[3] == 'done' for p in phases), '所有阶段最终应为 done')
        # 任务应正常完成，且存库 phases 为列表、reply 含各阶段结论
        self.assertEqual(mgr.get_task(tid)['status'], m.AIChatManager.STATUS_COMPLETED)
        stored = mgr.get_task(tid)['reply'] or ''
        self.assertIn('分析用户意图：这是一条', stored)
        self.assertIn('做事前检查', stored)
        self.assertIn('做事后检查', stored)
        # 存库 phases 字段为可解析的 JSON 列表，首阶段 label 正确
        ph = m._parse_phases(mgr.get_task(tid).get('phases'))
        self.assertIsInstance(ph, list)
        self.assertEqual(ph[0]['label'], '分析用户意图（建议 / 缺陷 / 继续 / 闲聊）')

    def test_sse_block_phase_format(self):
        block = m._sse_block('phase', '{"index":0,"label":"分析用户意图","kind":"host","state":"running"}')
        self.assertIn('event: phase', block)
        self.assertIn('data: {"index":0', block)

    def test_chat_branch_two_bubbles_no_git(self):
        # 闲聊意图：只走「分析用户意图 + 生成回复」两个阶段，不触发 git 核查与建单阶段。
        mgr = m.AIChatManager()
        tmp = tempfile.mkdtemp()
        mgr.init(tmp)
        with mock.patch.object(m.subprocess, 'Popen', lambda *a, **k: _FakeProc()), \
             mock.patch.object(m, '_resolve_buddy_cli', lambda: 'codebuddy'):
            import uuid
            tid = 'ai_' + uuid.uuid4().hex[:16]
            mgr._insert_task(tid, '你好，谢谢你的帮助', None, m.AIChatManager.STATUS_PENDING)
            events, phases = _collect_phases(mgr, tid)
        labels = [p[1] for p in phases]
        self.assertEqual(mgr.get_task(tid)['status'], m.AIChatManager.STATUS_COMPLETED)
        # 首项为意图判断、末项为生成回复（闲聊无 git 核查 / 分析 / 执行 / 处理完成 等任务型阶段）
        self.assertEqual(labels[0], '分析用户意图（建议 / 缺陷 / 继续 / 闲聊）')
        self.assertEqual(labels[-1], '生成回复')
        self.assertFalse(any('git 仓库状态' in s for s in labels))
        self.assertFalse(any('AI 分析定位' in s for s in labels))
        self.assertFalse(any('AI 执行处理' in s for s in labels))


if __name__ == '__main__':
    unittest.main()
