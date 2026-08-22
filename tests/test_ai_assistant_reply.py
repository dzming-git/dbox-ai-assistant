# -*- coding: utf-8 -*-
"""AI 助手回复构造（stdout 为空时回退采用 stderr 正文）测试。

验证：buddy 在部分运行环境（非 TTY 管道）下把最终回复写到 stderr 而非 stdout，
此前只读 stdout 导致正文被丢弃、频繁出现「（任务已执行完成，无文本输出）」。
_build_reply 必须在 stdout 为空且退出码正常时回退采用 stderr 正文。

运行：python tests/test_ai_assistant_reply.py
"""
import os
import sys
import unittest

_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'extensions_host'))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import ai_assistant as m


class BuildReplyTest(unittest.TestCase):
    def test_stdout_used_when_present(self):
        reply, fell_back = m._build_reply(['hello', 'world'], '', 0)
        self.assertEqual(reply, 'hello\nworld')
        self.assertFalse(fell_back)

    def test_fallback_to_stderr_when_stdout_empty(self):
        reply, fell_back = m._build_reply([], '模型的最终回复正文', 0)
        self.assertEqual(reply, '模型的最终回复正文')
        self.assertTrue(fell_back)

    def test_placeholder_when_both_empty(self):
        reply, fell_back = m._build_reply([], '', 0)
        self.assertEqual(reply, '')
        self.assertFalse(fell_back)

    def test_whitespace_stderr_not_used(self):
        reply, fell_back = m._build_reply([], '   \n  ', 0)
        self.assertEqual(reply, '')
        self.assertFalse(fell_back)

    def test_no_fallback_on_nonzero_returncode(self):
        # 退出码非 0 由调用方走失败分支；此处即便 stderr 有内容也不应作为回复采用
        reply, fell_back = m._build_reply([], 'some text', 1)
        self.assertEqual(reply, '')
        self.assertFalse(fell_back)

    def test_auth_error_not_fallback(self):
        # 认证错误由调用方拦截，_build_reply 不应把它当作正文采用
        reply, fell_back = m._build_reply([], '认证失败，请先登录', 0)
        self.assertEqual(reply, '')
        self.assertFalse(fell_back)


if __name__ == '__main__':
    unittest.main(verbosity=2)
