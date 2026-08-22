# -*- coding: utf-8 -*-
"""工作流引擎：配置驱动的 AI 助手流程框架。

设计目标（方案 2）：代码只提供框架，每个工作流的步骤 / 提示词 / 判定命令
全部由 extensions/ai_assistant/workflows/*.yaml 描述，便于独立维护与扩展。

工作流 step 三种 kind：
  - shell：运行 cmd，取 stdout 末行匹配 expect，命中则把 on_expect_prompt 注入系统提示
  - llm  ：把 prompt 直接并入系统提示（主流程指导）
  - ask  ：向 UI 提问并挂起，等用户选择 options 之一，按 inject_on 注入

step.when 三阶段：
  - start：进入队列、真正调用 CodeBuddy 之前执行（查 git / 建单提问）
  - main ：主体提示（可多个）
  - end  ：回复生成之后执行（复查 git）

实时推断：仅在用户未手动选择、且前端未带 workflow_id 时，对 auto_infer=true 的工作流
调用一次轻量分类（由外部注入的 cb_classify 回调完成），不可行时回落 chat。
"""
import os
import json
import subprocess
import threading
import time

try:
    import yaml
except ImportError:
    yaml = None

WORKFLOWS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),  # backend/ 的父目录 = ai_assistant/
    'workflows'
)

DEFAULT_WORKFLOW = 'chat'

# shell 步允许的命令前缀白名单（安全限制，避免任意命令执行）
SHELL_ALLOW_PREFIXES = ('git ', 'cmd /c "git ')


class WorkflowEngine:
    def __init__(self, directory=WORKFLOWS_DIR):
        self.directory = directory
        self._lock = threading.RLock()
        self._cache = None          # {id: dict}
        self._cache_mtime = {}      # filename -> mtime

    # ---------- 加载 ----------
    def load_workflows(self, force=False):
        with self._lock:
            if not force and self._cache is not None:
                # 检查文件变更做热重载
                if not self._need_reload():
                    return self._cache
            flows = {}
            if not os.path.isdir(self.directory):
                self._cache = flows
                return flows
            for fn in os.listdir(self.directory):
                if not fn.endswith(('.yaml', '.yml')):
                    continue
                path = os.path.join(self.directory, fn)
                try:
                    mtime = os.path.getmtime(path)
                    self._cache_mtime[fn] = mtime
                    data = self._read_yaml(path)
                    if data and data.get('id'):
                        flows[data['id']] = data
                except Exception:
                    continue
            self._cache = flows
            return flows

    def _need_reload(self):
        if not os.path.isdir(self.directory):
            return False
        for fn in os.listdir(self.directory):
            if not fn.endswith(('.yaml', '.yml')):
                continue
            path = os.path.join(self.directory, fn)
            try:
                if os.path.getmtime(path) != self._cache_mtime.get(fn):
                    return True
            except OSError:
                return True
        return False

    def _read_yaml(self, path):
        if yaml is None:
            return self._read_yaml_simple(path)
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _read_yaml_simple(self, path):
        """极简 yaml 解析兜底（仅支持本配置用到的结构）：
        顶层层、steps 列表、- kind/key: value、嵌套 prompt/inject_on 等。
        优先用 PyYAML；此函数仅在环境无 yaml 时启用。"""
        import re
        data = {}
        steps = []
        cur = None
        cur_sub = None  # 当前 step 内的嵌套块（on_expect_prompt/inject_on/prompt）
        buf = []
        with open(path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        indent_of = lambda s: len(s) - len(s.lstrip(' '))

        def flush_buf():
            nonlocal buf
            if cur_sub is not None and buf:
                text = '\n'.join(buf).rstrip('\n')
                cur[cur_sub] = text
                buf = []

        for raw in lines:
            line = raw.rstrip('\n')
            if not line.strip() or line.strip().startswith('#'):
                continue
            ind = indent_of(line)
            if line.lstrip().startswith('- kind:'):
                flush_buf()
                cur = {'kind': line.split('kind:', 1)[1].strip().strip('"')}
                steps.append(cur)
                cur_sub = None
                continue
            if cur is not None and ind >= 4 and (line.strip().startswith('id:') or
                                                 line.strip().startswith('when:') or
                                                 line.strip().startswith('cmd:') or
                                                 line.strip().startswith('expect:') or
                                                 line.strip().startswith('question:') or
                                                 line.strip().startswith('options:')):
                flush_buf()
                k, v = line.strip().split(':', 1)
                cur[k.strip()] = v.strip().strip('"')
                continue
            if cur is not None and ind >= 4 and line.strip().startswith('on_expect_prompt:'):
                flush_buf(); cur_sub = 'on_expect_prompt'; buf = []; continue
            if cur is not None and ind >= 4 and line.strip().startswith('prompt:'):
                flush_buf(); cur_sub = 'prompt'; buf = []; continue
            if cur is not None and ind >= 4 and line.strip().startswith('inject_on:'):
                flush_buf(); cur_sub = 'inject_on'; buf = []; cur['inject_on'] = {}; continue
            if cur is not None and cur_sub == 'inject_on' and line.strip().startswith('"') and ':' in line:
                flush_buf()
                k, v = line.strip().split(':', 1)
                cur['inject_on'][k.strip().strip('"')] = v.strip().strip('"')
                continue
            if cur is not None and cur_sub in ('on_expect_prompt', 'prompt') and ind >= 8:
                buf.append(line.strip())
                continue
            # 顶层键值
            if ind == 0 and ':' in line:
                flush_buf()
                k, v = line.split(':', 1)
                data[k.strip()] = v.strip().strip('"')
        flush_buf()
        data['steps'] = steps
        return data

    def get(self, wf_id):
        flows = self.load_workflows()
        return flows.get(wf_id) or flows.get(DEFAULT_WORKFLOW)

    def list_meta(self):
        """给前端渲染选择面板用的元信息。"""
        flows = self.load_workflows()
        out = []
        for fid, f in flows.items():
            out.append({
                'id': fid,
                'name': f.get('name', fid),
                'icon': f.get('icon', '•'),
                'color': f.get('color', '#5b6470'),
                'description': f.get('description', ''),
                'auto_infer': bool(f.get('auto_infer', False)),
            })
        return out

    # ---------- 实时推断 ----------
    def infer_workflow(self, message, history, cb_classify=None):
        """返回 workflow id 或 None（不可推断时回落由调用方处理）。
        cb_classify(system_prompt, user_message) -> 文本（调用 CodeBuddy 分类）。"""
        flows = self.load_workflows()
        candidates = [f for f in flows.values() if f.get('auto_infer', False)]
        if not candidates:
            return DEFAULT_WORKFLOW
        hint = '\n'.join('- %s：%s' % (f['id'], f.get('infer_hint', '').strip())
                         for f in candidates)
        system = (
            '你是意图分类器。下面是可选项及其判定说明：\n%s\n\n'
            '只输出一个最匹配的 id（严格从上述 id 中选，不要解释、不要多余字符）。'
            '若都不匹配输出 %s。' % (hint, DEFAULT_WORKFLOW)
        )
        if cb_classify is None:
            return None
        try:
            resp = cb_classify(system, message).strip()
        except Exception:
            return None
        # 取最后一个非空 token，容错
        for tok in reversed(resp.split()):
            tok = tok.strip().strip('`').strip('"')
            if tok in flows:
                return tok
        return None

    # ---------- 编译系统提示 ----------
    def compile_prompt(self, wf, answers=None, hit_shells=None):
        """拼装该工作流主流程的提示词段。
        answers: {step_id: 用户选择}；hit_shells: {step_id: True}（已命中 expect 的 shell 步）。"""
        answers = answers or {}
        hit_shells = hit_shells or {}
        parts = []
        name = wf.get('name', wf.get('id'))
        parts.append('【当前工作流：%s】%s' % (name, wf.get('description', '')))
        for step in wf.get('steps', []):
            if step.get('kind') == 'llm':
                p = step.get('prompt')
                if p:
                    parts.append(p.strip())
            elif step.get('kind') == 'shell':
                if hit_shells.get(step.get('id')) and step.get('on_expect_prompt'):
                    parts.append(step['on_expect_prompt'].strip())
            elif step.get('kind') == 'ask':
                sid = step.get('id')
                if sid in answers:
                    choice = answers[sid]
                    inj = step.get('inject_on', {})
                    if choice in inj:
                        parts.append(inj[choice].strip())
        return '\n\n'.join(parts)

    # ---------- shell 步执行 ----------
    def run_shell_step(self, step, cwd=None, timeout=30):
        """运行 shell 步，返回 (hit: bool, stdout_text: str)。"""
        cmd = step.get('cmd', '').strip()
        if not cmd:
            return False, ''
        if not self._shell_allowed(cmd):
            return False, 'BLOCKED: command not allowed'
        try:
            proc = subprocess.run(cmd, shell=True, cwd=cwd,
                                  capture_output=True, text=True, timeout=timeout)
            out = (proc.stdout or '').strip()
        except subprocess.TimeoutExpired:
            return False, 'TIMEOUT'
        except Exception as e:
            return False, 'ERR:%s' % e
        last = out.splitlines()[-1] if out else ''
        expect = str(step.get('expect', '')).strip()
        hit = (expect in last) or (expect == last)
        return hit, out

    def _shell_allowed(self, cmd):
        c = cmd.lower()
        return any(c.startswith(p.lower()) for p in SHELL_ALLOW_PREFIXES)

    # ---------- 取某阶段的步骤 ----------
    def steps_of(self, wf, when):
        return [s for s in wf.get('steps', []) if s.get('when') == when]

    def ask_steps(self, wf):
        return [s for s in wf.get('steps', []) if s.get('kind') == 'ask']


# 模块级单例（在 ai_assistant.py 中复用）
_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = WorkflowEngine()
    return _engine
