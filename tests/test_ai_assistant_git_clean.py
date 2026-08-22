# -*- coding: utf-8 -*-
"""AI 助手 git 仓库干净度核查（结构性保证）测试。

验证：处理前快照基线、处理后由进程客观比对工作树，对本次运行「新增」的脏文件
告警，并对符合项目规范的临时文件（_commit_msg.txt / *.tmp 等）自动清理。

运行：python tests/test_ai_assistant_git_clean.py
"""
import os
import sys
import shutil
import subprocess
import tempfile
import unittest

_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'extensions_host'))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import ai_assistant as m


def _git(repo, *args):
    subprocess.run(['git'] + list(args), cwd=repo,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)


def _make_repo():
    """创建一个最小 git 仓库（含一个已提交的基线文件），返回路径。"""
    path = tempfile.mkdtemp(prefix='ai_git_clean_')
    _git(path, 'init', '-q')
    _git(path, 'config', 'user.name', 'tester')
    _git(path, 'config', 'user.email', 'tester@example.com')
    with open(os.path.join(path, 'base.txt'), 'w') as f:
        f.write('base')
    _git(path, 'add', 'base.txt')
    _git(path, 'commit', '-q', '-m', 'init')
    return path


class GitCleanTest(unittest.TestCase):

    def test_clean_repo_returns_clean(self):
        repo = _make_repo()
        try:
            baseline = m._git_dirty_files(repo)
            reply, clean = m._verify_and_report_clean(repo, baseline, None, 'done')
            self.assertTrue(clean)
            self.assertEqual(reply, 'done')
        finally:
            shutil.rmtree(repo, ignore_errors=True)

    def test_temp_file_auto_cleaned(self):
        repo = _make_repo()
        try:
            tmp = os.path.join(repo, '_commit_msg.txt')
            with open(tmp, 'w') as f:
                f.write('x')
            baseline = set()
            reply, clean = m._verify_and_report_clean(repo, baseline, None, 'done')
            self.assertTrue(clean)
            self.assertFalse(os.path.exists(tmp))
            self.assertIn('自动清理临时文件', reply)
        finally:
            shutil.rmtree(repo, ignore_errors=True)

    def test_real_dirty_file_reported_not_clean(self):
        repo = _make_repo()
        try:
            real = os.path.join(repo, 'real_change.py')
            with open(real, 'w') as f:
                f.write('print(1)')
            baseline = set()
            reply, clean = m._verify_and_report_clean(repo, baseline, None, 'done')
            self.assertFalse(clean)
            self.assertIn('git 仓库未保持干净', reply)
            self.assertIn('real_change.py', reply)
            # 真实改动不应被自动删除
            self.assertTrue(os.path.exists(real))
        finally:
            shutil.rmtree(repo, ignore_errors=True)

    def test_baseline_dirty_not_attributed(self):
        repo = _make_repo()
        try:
            pre = os.path.join(repo, 'preexisting.py')
            with open(pre, 'w') as f:
                f.write('old')
            # 运行前基线已含该脏文件
            baseline = m._git_dirty_files(repo)
            self.assertIn('preexisting.py', baseline)
            reply, clean = m._verify_and_report_clean(repo, baseline, None, 'done')
            # 本次未新增脏文件 → 视为干净（仅温和提醒基线遗留）
            self.assertTrue(clean)
            self.assertIn('任务开始前仓库即存在未提交改动', reply)
        finally:
            shutil.rmtree(repo, ignore_errors=True)

    def test_new_dirty_minus_baseline_only(self):
        repo = _make_repo()
        try:
            pre = os.path.join(repo, 'preexisting.py')
            with open(pre, 'w') as f:
                f.write('old')
            baseline = m._git_dirty_files(repo)
            # 本次新增一个真实改动
            newf = os.path.join(repo, 'added.py')
            with open(newf, 'w') as f:
                f.write('new')
            reply, clean = m._verify_and_report_clean(repo, baseline, None, 'done')
            self.assertFalse(clean)
            self.assertIn('added.py', reply)
            # 仅「新增」脏文件进入主告警列表；运行前基线残留只在末尾提示中出现
            self.assertIn('本次任务遗留了未提交的改动', reply)
            self.assertNotIn('preexisting.py', reply.split('（提示')[0])
        finally:
            shutil.rmtree(repo, ignore_errors=True)

    def test_non_git_repo_skips(self):
        path = tempfile.mkdtemp(prefix='ai_notgit_')
        try:
            reply, clean = m._verify_and_report_clean(path, None, None, 'done')
            self.assertTrue(clean)
            self.assertEqual(reply, 'done')
        finally:
            shutil.rmtree(path, ignore_errors=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
