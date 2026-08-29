# -*- coding: utf-8 -*-
"""验证 CodeBuddy自动建单的结构：标题=概括、内容=问题描述、留言=AI 处理动作。

此前自动跟踪单的标题是「AI 处理：<原始诉求首行>」、内容里混入了「AI 做了什么」，
且完全没有留言。本测试确认：
- 标题为对问题的概括（剥离命令词/截断），不再照抄原始诉求；
- 内容为问题描述（用户诉求原文）；
- AI 的处理说明作为「自动助手」身份的首条留言写入 feedback_comments。
"""
import os
import sys
import json
import tempfile
import importlib
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, '..', 'src', 'extensions_host'))
sys.path.insert(0, os.path.join(_HERE, '..', 'src', 'web', 'backend'))

import codebuddy as ac
import platform_client as pc


def test_make_ticket_title_summarizes():
    # 剥离开头命令词，保留问题本质
    assert ac._make_ticket_title('你来解决这个问题：下载服务状态显示异常') == '下载服务状态显示异常'
    # 按断句截断到首句
    assert ac._make_ticket_title('图集列表空白。点赞收藏页也空白') == '图集列表空白'
    # 空诉求兜底
    assert ac._make_ticket_title('   ') == 'AI 处理任务'
    # 超长截断
    long_p = '稍后再看的视频反复复活且' + '很长的描述' * 20
    t = ac._make_ticket_title(long_p)
    assert len(t) <= 41 and t.endswith('…')


def test_tracking_ticket_structure_mocked():
    """不落库：确认 _maybe_create_tracking_ticket 把分析/解决作为自动助手留言传给建单接口。"""
    captured = {}

    def fake_file_feedback(ftype, title, content, extra=None, status='open',
                           comment=None, comments=None):
        captured['call'] = dict(ftype=ftype, title=title, content=content,
                                extra=extra, status=status, comment=comment,
                                comments=comments)
        return '202608130001'

    prompt = '你来解决这个问题：稍后再看里有个很早的视频删了无数次还反复出现'
    analysis = ('## 分析\n硬删除 + 列表按可见性隐藏，导致「稍后再看」里的视频被重新列出。')
    resolution = ('## 修复\n改为服务端软删除，并在列表查询中排除软删除项。')

    with mock.patch.object(pc, 'file_feedback', fake_file_feedback), \
         mock.patch.object(pc, 'search_feedback_issues', return_value=[]), \
         mock.patch.object(ac, '_git_rev_head', return_value='newcommit123'):
        out_reply, track_id, _ = ac._maybe_create_tracking_ticket(
            'task-1', prompt, 'owner-1', resolution, None, head_before='oldcommit',
            git_clean=True, analysis=analysis, resolution=resolution)

    assert track_id == '202608130001'
    call = captured['call']
    # 标题应为概括，而非 "AI 处理：<原始首行>"
    assert not call['title'].startswith('AI 处理：')
    assert call['title'] == '稍后再看里有个很早的视频删了无数次还反复出现'
    # 内容=问题描述（用户诉求原文）
    assert call['content'] == prompt
    # 留言1（首条）= 分析根因；留言2（后续）= 解决说明；均为「自动助手」身份
    assert call['comment'] == analysis
    assert call['comments'] == [resolution]
    assert call['status'] == 'pending_verification'
    assert call['extra']['git_commit'] == 'newcommit123'
    assert call['extra']['git_clean'] is True
    # 回复末尾被追加了跟踪单提示
    assert '📋 已创建处理跟踪单：#202608130001' in out_reply


def test_tracking_ticket_writes_comment_to_db():
    """落库：确认建单后反馈单含「分析 + 解决」两条「自动助手」身份留言。"""
    tmp = tempfile.mkdtemp()
    os.environ['DBOX_DATA_DIR'] = tmp
    import feedback_db
    importlib.reload(feedback_db)
    feedback_db.init_feedback_db()

    captured = {}

    def fake_file_feedback(ftype, title, content, extra=None, status='open',
                           comment=None, comments=None):
        # 模拟主服务内部接口：直接落库（含留言）
        captured['call'] = dict(title=title, content=content, comment=comment,
                                comments=comments, status=status)
        issue_id = feedback_db.db_create_issue(
            title=title, content=content, category=ftype,
            submitter='自动助手', source='codebuddy', auto_classified=True,
            status=status, extra=extra, comment=comment, comments=comments)
        return issue_id

    prompt = '反馈中心自动建单逻辑有问题，标题应该是概括'
    analysis = '分析：建单时漏传 analysis/resolution，导致分析根因未写入留言。'
    resolution = '解决：调用处补传 analysis=analysis, resolution=reply。'

    with mock.patch.object(pc, 'file_feedback', fake_file_feedback), \
         mock.patch.object(pc, 'search_feedback_issues', return_value=[]), \
         mock.patch.object(ac, '_git_rev_head', return_value='commitabc'):
        out_reply, track_id, _ = ac._maybe_create_tracking_ticket(
            'task-2', prompt, 'owner-2', resolution, None, head_before='commitold',
            git_clean=True, analysis=analysis, resolution=resolution)

    assert track_id
    # 在 session 内读取，避免 detached 懒加载报错
    with feedback_db.get_session() as session:
        from feedback_db import FeedbackIssue
        issue = session.get(FeedbackIssue, track_id)
        assert issue is not None
        # 概括：剥离命令词、按句号截断、超长截断；此处不足 40 字故保留原诉求
        assert issue.title == '反馈中心自动建单逻辑有问题，标题应该是概括'
        assert issue.content == prompt                  # 问题描述
        comments = list(issue.comments)                 # 触发懒加载并物化
    assert len(comments) == 2
    # 首条=分析根因，次条=解决说明
    assert comments[0].content == analysis
    assert comments[1].content == resolution
    for c in comments:
        assert c.author == '自动助手'
        assert c.author_role == 2
    # 清理临时库
    try:
        os.remove(feedback_db.FEEDBACK_DB_PATH)
    except Exception:
        pass


def test_extract_ref_issue():
    """从用户消息里提取被引用的既有反馈单号（形如 #202608130018）。"""
    assert ac._extract_ref_issue('继续处理 #202608130018 这个单') == '202608130018'
    assert ac._extract_ref_issue('请看一下反馈 #202608120001') == '202608120001'
    assert ac._extract_ref_issue('正常任务没有单号') is None
    assert ac._extract_ref_issue('#12345') is None          # 不足 12 位不算单号
    assert ac._extract_ref_issue('') is None


def test_add_feedback_comment_to_existing_ticket():
    """引用既有单时：分析/解决应以自动助手身份追加进该单（而非另建跟踪单）。"""
    calls = []

    def fake_add(issue_id, content):
        calls.append((issue_id, content))
        return True

    with mock.patch.object(pc, 'add_feedback_comment', fake_add):
        assert ac._add_feedback_comment('202608130018', '分析：根因是 X') is True
        assert ac._add_feedback_comment('202608130018', '   ') is False  # 空内容不落库
    assert calls == [('202608130018', '分析：根因是 X')]


def test_process_replies_to_referenced_ticket():
    """用户引用既有反馈单时，阶段6 把分析/解决以自动助手身份回复进该单。"""
    import queue as _queue
    import tempfile as _tf
    import uuid as _uuid

    class _FakeProc:
        def __init__(self):
            import io
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

    mgr = ac.AIChatManager()
    mgr.init(_tf.mkdtemp())
    captured = []
    def fake_add(issue_id, content):
        captured.append(content)
        return True

    with mock.patch.object(ac.subprocess, 'Popen', lambda *a, **k: _FakeProc()), \
         mock.patch.object(ac, '_resolve_buddy_cli', lambda: 'codebuddy'), \
         mock.patch.object(ac, '_git_rev_head', side_effect=['oldc', 'newc']), \
         mock.patch.object(ac, '_add_feedback_comment', fake_add):
        tid = 'ai_' + _uuid.uuid4().hex[:16]
        mgr._insert_task(tid, '继续处理一下 #202608130018 这个反馈单', None,
                         ac.AIChatManager.STATUS_PENDING)
        q = _queue.Queue()
        with mgr._lock:
            mgr._subscribers[tid] = [q]
        mgr._process(tid)

    assert captured, '应把分析与解决回复进被引用的反馈单'
    # 阶段3 分析 + 阶段4 执行各产生一条回复（fake 正文相同）
    assert len(captured) == 2
    assert mgr.get_task(tid)['status'] == ac.AIChatManager.STATUS_COMPLETED


def test_find_existing_ticket_detects_duplicate():
    """反馈中心已有覆盖同一问题的未关闭单时，应返回该单号。"""
    cands = [{'id': '202608130099', 'title': '竖屏刷新后视频看不到',
              'content': '竖屏模式下触发浏览器刷新动作后视频就看不到了', 'status': 'pending_verification'},
            {'id': '202608130100', 'title': '其它无关问题', 'content': '完全不同的诉求', 'status': 'open'}]
    with mock.patch.object(pc, 'search_feedback_issues', return_value=cands):
        hit = ac._find_existing_ticket('竖屏模式下触发浏览器刷新动作后视频就看不到了', 'bug')
    assert hit == '202608130099'


def test_find_existing_ticket_no_candidate():
    """没有任何候选单时返回 None。"""
    with mock.patch.object(pc, 'search_feedback_issues', return_value=[]):
        assert ac._find_existing_ticket('竖屏模式下触发浏览器刷新动作后视频就看不到了', 'bug') is None


def test_find_existing_ticket_short_prompt():
    """过短诉求不触发去重（避免噪声误合并）。"""
    with mock.patch.object(pc, 'search_feedback_issues', return_value=[]):
        assert ac._find_existing_ticket('视频看不到了', 'bug') is None


def test_find_existing_ticket_unrelated_no_match():
    """候选单内容与该诉求无关时不误合并。"""
    cands = [{'id': '202608130101', 'title': '首页配色偏暗',
              'content': '建议把首页背景调亮一点', 'status': 'open'}]
    with mock.patch.object(pc, 'search_feedback_issues', return_value=cands):
        assert ac._find_existing_ticket('竖屏模式下触发浏览器刷新动作后视频就看不到了', 'bug') is None


def test_maybe_create_reuses_existing_ticket():
    """命中已有未关闭单时：续写旧单（追加分析+解决留言）而非新建跟踪单。"""
    existing = '202608130099'
    cands = [{'id': existing, 'title': '竖屏刷新后视频看不到',
              'content': '竖屏模式下触发浏览器刷新动作后视频就看不到了', 'status': 'pending_verification'}]
    analysis = '分析：刷新后未重新拉起视频播放器。'
    resolution = '解决：刷新后重建播放器实例。'

    file_calls = []
    comment_calls = []

    def fake_file(ftype, title, content, extra=None, status='open',
                  comment=None, comments=None):
        file_calls.append((ftype, title))
        return '202608139999'

    def fake_add(issue_id, content):
        comment_calls.append((issue_id, content))
        return True

    with mock.patch.object(pc, 'search_feedback_issues', return_value=cands), \
         mock.patch.object(pc, 'add_feedback_comment', fake_add), \
         mock.patch.object(pc, 'file_feedback', fake_file), \
         mock.patch.object(ac, '_git_rev_head', return_value='newcommit123'):
        out_reply, track_id, s_ticket = ac._maybe_create_tracking_ticket(
            'task-x', '竖屏模式下触发浏览器刷新动作后视频就看不到了', 'owner-x',
            resolution, None, head_before='oldcommit', git_clean=True,
            analysis=analysis, resolution=resolution)

    # 不应新建跟踪单
    assert file_calls == []
    # 应把分析+解决续写进旧单
    assert track_id == existing
    assert len(comment_calls) == 2
    assert comment_calls[0] == (existing, analysis)
    assert comment_calls[1] == (existing, resolution)
    # 回复中应说明已续接旧单、未重复建单
    assert '#%s' % existing in out_reply
    assert '未重复建单' in out_reply

