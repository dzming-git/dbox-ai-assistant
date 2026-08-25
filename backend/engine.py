"""AI 助手对话引擎（归零版）。

设计原则：回到最原始的聊天体验——就像在 IDE 里和助手对话一样。
- 纯多轮文本对话，流式输出（SSE）。
- 底层经 CodeBuddy CLI（-p 非交互 + stream-json）调用，保留其文件读写/命令执行能力。
- 会话历史由服务端按 owner 维护（内存），每次请求把最近若干轮拼成纯文本发给 CLI。
- 不引入意图分类、阶段气泡、计划模式、反馈单建单、回滚、上下文续写等任何包裹逻辑，
  从根本上消除"上下文污染"与"处处受限"。

运行于独立的拓展宿主进程（extensions_host），凭证经 host.vault 读取。
"""
import os
import sys
import re
import json
import time
import uuid
import queue
import threading
import subprocess
import logging

_logger = logging.getLogger('extensions_host.ai_assistant')

try:
    from subprocess import CREATE_NEW_PROCESS_GROUP
except ImportError:
    CREATE_NEW_PROCESS_GROUP = 0

# 单个 AI 任务的最大执行时长（秒）。超时由看门狗强制结束进程树，防止子进程
# stdout 管道不关闭导致 worker 永久阻塞、队列卡死。
_MAX_TASK_SECONDS = 600

# 每轮对话回传给 CLI 的最大历史条数（用户+助手各算一条）。
_MAX_HISTORY = 20

# 极简系统提示：直接动手、中文说明，仅此而已。
_SYSTEM_PROMPT = (
    '你是嵌入在媒体库管理后台里的 AI 助手，拥有读写文件、运行命令的真实能力。\n'
    '当用户布置具体任务（如修改代码、创建/删除文件、执行命令等）时，请直接动手完成，'
    '不要只罗列步骤或描述做法；完成后用简简体中文简要说明你做了什么。\n'
    '若只是闲聊或提问，则正常简洁回答即可。\n'
    '对话历史会在每次请求时整体回传给你，请基于上下文连续作答。'
)

# 可选模型（与 codebuddy CLI --model 对齐）。
AI_MODELS = [
    {'id': 'hy3', 'name': 'Hybrid 3'},
    {'id': 'deepseek-v4-pro', 'name': 'DeepSeek V4 Pro'},
    {'id': 'deepseek-v4-flash', 'name': 'DeepSeek V4 Flash'},
    {'id': 'glm-5.3', 'name': 'GLM 5.3'},
    {'id': 'kimi-k3-2', 'name': 'Kimi K3.2'},
]


def list_models():
    return list(AI_MODELS)


# ----------------------------------------------------------------------------
# CLI / 凭证辅助
# ----------------------------------------------------------------------------
_CODEBUDDY_API_KEY_ENV = 'CODEBUDDY_API_KEY'
_ANTHROPIC_API_KEY_ENV = 'ANTHROPIC_API_KEY'
_CODEBUDDY_ENV_ENV = 'CODEBUDDY_INTERNET_ENVIRONMENT'
_CODEBUDDY_TOKEN_DOMAIN = 'codebuddy'
_CODEBUDDY_INTERNET_ENVIRONMENT = 'internal'


def _load_codebuddy_token() -> str:
    for env_name in (_CODEBUDDY_API_KEY_ENV, _ANTHROPIC_API_KEY_ENV):
        env_token = os.environ.get(env_name)
        if env_token:
            return env_token.strip()
    vault = getattr(_ai_mgr_singleton, '_vault', None)
    if vault is not None:
        try:
            tok = vault.get(_CODEBUDDY_TOKEN_DOMAIN)
            if tok:
                return tok.strip()
        except Exception:
            pass
        try:
            for rec in vault.list_all():
                if rec.get('kind') == 'token' and 'codebuddy' in (rec.get('name') or '').lower():
                    return (rec.get('value') or '').strip()
        except Exception:
            pass
    return ''


def _project_root() -> str:
    """向上查找首个含 .git 的目录作为项目根（插件位于 dbox 仓库内）。"""
    env_root = os.environ.get('DBOX_PROJECT_ROOT') or os.environ.get('DBOX_REPO_ROOT')
    if env_root and os.path.isdir(os.path.join(env_root, '.git')):
        return env_root
    d = os.path.dirname(os.path.abspath(__file__))
    while True:
        if os.path.isdir(os.path.join(d, '.git')):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            return os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))))))
        d = parent


def _resolve_buddy_cli() -> str:
    cands = []
    env_buddy = os.environ.get('DBOX_BUDDYCN')
    if env_buddy:
        cands.append(env_buddy)
    appdata = os.environ.get('APPDATA')
    if appdata:
        cands.append(os.path.join(appdata, 'npm', 'codebuddy.cmd'))
    for uname in ('71555',):
        cands.append(r'C:\Users\%s\AppData\Roaming\npm\codebuddy.cmd' % uname)
        cands.append(r'C:\Users\%s\AppData\Local\npm\codebuddy.cmd' % uname)
    try:
        import shutil
        on_path = shutil.which('codebuddy.cmd') or shutil.which('codebuddy')
        if on_path:
            cands.append(on_path)
    except Exception:
        pass
    seen = set()
    for c in cands:
        if not c or c in seen:
            continue
        seen.add(c)
        if os.path.isfile(c):
            return c
    return ''


def _codebuddy_user_home() -> str:
    env_home = os.environ.get('DBOX_BUDDYCN_HOME')
    if env_home and os.path.isdir(env_home):
        return env_home
    for uname in ('71555',):
        home = r'C:\Users\%s' % uname
        if os.path.isdir(home):
            return home
    return ''


def _sse_block(event: str, data) -> str:
    data = '' if data is None else str(data)
    lines = data.split('\n')
    return 'event: %s\n' % event + ''.join('data: ' + ln + '\n' for ln in lines) + '\n'


# ----------------------------------------------------------------------------
# 单例（供辅助函数惰性读取宿主注入的 vault）
# ----------------------------------------------------------------------------
_ai_mgr_singleton = None


class AIChatManager:
    STATUS_PENDING = 'pending'
    STATUS_RUNNING = 'running'
    STATUS_COMPLETED = 'completed'
    STATUS_FAILED = 'failed'
    STATUS_CANCELLED = 'cancelled'

    def __init__(self):
        self._lock = threading.RLock()
        self._queue = queue.Queue()          # FIFO：仅存待执行的 task_id
        self._worker = None
        self._procs = {}                      # task_id -> Popen（取消用）
        self._cancel = {}                     # task_id -> True
        self._convs = {}                      # owner_id -> [ {role,text}, ... ]（纯多轮历史）
        self._subscribers = {}                # task_id -> [queue.Queue, ...]（SSE 订阅者）
        self._buffers = {}                    # task_id -> [event_str, ...]（供重连续接）
        self._tasks = {}                      # task_id -> {owner_id, status, created_at}
        self._vault = None
        self._initialized = False

    # ---------- 初始化 ----------
    def init(self, data_dir, vault=None, tasks=None):
        self._vault = vault
        with self._lock:
            if self._initialized:
                return
            self._initialized = True
        self._start_worker()
        _logger.info('AI 助手引擎已初始化（归零版：纯多轮对话）')

    def _start_worker(self):
        if self._worker and self._worker.is_alive():
            return
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()

    # ---------- 历史 ----------
    def _history(self, owner_id):
        return self._convs.setdefault(owner_id, [])

    def history(self, owner_id, limit=50):
        h = self._history(owner_id)
        return h[-limit:]

    def clear(self, owner_id=None):
        if owner_id is None:
            self._convs.clear()
        else:
            self._convs.pop(owner_id, None)

    # ---------- 入队 ----------
    def chat(self, message, owner_id=None, model=None):
        """入队一条用户消息，返回 task_id。会在历史里追加该用户消息。"""
        message = (message or '').strip()
        if not message:
            return None, '消息为空'
        task_id = uuid.uuid4().hex
        with self._lock:
            self._tasks[task_id] = {
                'owner_id': owner_id, 'status': self.STATUS_PENDING,
                'created_at': time.time(), 'model': model,
            }
            self._subscribers[task_id] = []
            self._buffers[task_id] = []
            h = self._history(owner_id)
            h.append({'role': 'user', 'text': message})
        self._queue.put(task_id)
        return task_id, None

    # ---------- SSE 订阅 ----------
    def subscribe(self, task_id):
        with self._lock:
            if task_id not in self._tasks:
                return iter(['event: error\ndata: 任务不存在\n\n'])
            buf = list(self._buffers.get(task_id, []))
            sub_q = queue.Queue()
            self._subscribers.setdefault(task_id, []).append(sub_q)
        # 先回放已产生的缓冲（重连续接）
        for blk in buf:
            yield blk

        # 流式后续
        while True:
            blk = sub_q.get()
            if blk is None:
                return
            yield blk

    def _emit(self, task_id, block):
        with self._lock:
            self._buffers.setdefault(task_id, []).append(block)
            subs = list(self._subscribers.get(task_id, []))
        for q in subs:
            q.put(block)

    # ---------- 取消 ----------
    def cancel(self, task_id):
        with self._lock:
            if task_id not in self._tasks:
                return False
            self._cancel[task_id] = True
            proc = self._procs.get(task_id)
        if proc is not None:
            self._terminate(proc)
        return True

    def _terminate(self, proc):
        try:
            pid = proc.pid
            if os.name == 'nt':
                subprocess.run(['taskkill', '/F', '/T', '/PID', str(pid)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            else:
                import signal
                os.killpg(os.getpgid(pid), signal.SIGKILL)
        except Exception:
            pass

    # ---------- 状态 ----------
    def status(self, task_id):
        with self._lock:
            return self._tasks.get(task_id, {}).get('status')

    # ---------- 忙碌态轮询（供框架层 pollBusy 使用） ----------
    def tasks_state(self, owner_id):
        """返回该用户任务概览，供宿主侧轻量轮询判断是否忙碌/有未读。

        响应结构对齐 ExtensionHost.vue 的 pollBusy：
          - active: 是否有正在执行的任务
          - pending: 排队中的任务 id 列表
          - history: 已结束任务列表（每项含 id，用于未读检测）
        """
        with self._lock:
            owned = [(tid, t) for tid, t in self._tasks.items()
                     if t.get('owner_id') == owner_id]
            active = any(t['status'] == self.STATUS_RUNNING for _, t in owned)
            pending = [tid for tid, t in owned if t['status'] == self.STATUS_PENDING]
            history = [
                {'id': tid, 'status': t['status']}
                for tid, t in owned
                if t['status'] in (self.STATUS_COMPLETED, self.STATUS_FAILED)
            ]
        return {'active': active, 'pending': pending, 'history': history}

    # ---------- worker ----------
    def _worker_loop(self):
        while True:
            task_id = self._queue.get()
            if task_id is None:
                break
            try:
                self._run_task(task_id)
            except Exception as e:
                _logger.exception('AI 任务执行异常: %s', e)
                self._emit(task_id, _sse_block('error', '执行异常：%s' % e))
                self._emit(task_id, _sse_block('done', 'failed'))
            finally:
                with self._lock:
                    self._procs.pop(task_id, None)
                self._emit(task_id, _sse_block('done', 'closed'))

    def _run_task(self, task_id):
        with self._lock:
            info = self._tasks.get(task_id, {})
            owner_id = info.get('owner_id')
            model = info.get('model')
            info['status'] = self.STATUS_RUNNING
        # 构造 prompt：system + 最近若干轮历史 + 当前用户消息
        with self._lock:
            h = list(self._history(owner_id))
        recent = h[-(_MAX_HISTORY * 2):] if _MAX_HISTORY else h
        parts = [_SYSTEM_PROMPT, '']
        for turn in recent:
            role = '用户' if turn['role'] == 'user' else '助手'
            parts.append('%s：%s' % (role, turn['text']))
        prompt = '\n'.join(parts)

        self._emit(task_id, _sse_block('queue', '0'))

        ok, reply, err, rc = self._call_cli(task_id, prompt, model)
        if not ok:
            self._emit(task_id, _sse_block('error', err or '调用失败'))
            with self._lock:
                self._tasks[task_id]['status'] = self.STATUS_FAILED
            return

        final = (reply or '').strip()
        if not final:
            final = '（助手未返回内容）'
        # 把助手回复追加进历史（仅最终正文，不含思考过程）
        with self._lock:
            self._history(owner_id).append({'role': 'assistant', 'text': final})
            self._tasks[task_id]['status'] = self.STATUS_COMPLETED
        self._emit(task_id, _sse_block('assistant', final))

    def _call_cli(self, task_id, prompt, model):
        """调用 CodeBuddy CLI（-p + stream-json），把 text_delta/thinking_delta 流式推送。"""
        buddy = _resolve_buddy_cli()
        if not buddy:
            return False, None, '未找到 CodeBuddy CLI', 1
        env = dict(os.environ)
        token = _load_codebuddy_token()
        if token:
            env[_CODEBUDDY_API_KEY_ENV] = token
            env[_ANTHROPIC_API_KEY_ENV] = token
        env.setdefault(_CODEBUDDY_ENV_ENV, _CODEBUDDY_INTERNET_ENVIRONMENT)
        _home = _codebuddy_user_home()
        if _home and os.path.isdir(_home):
            env['USERPROFILE'] = _home
            env['HOME'] = _home
            env['APPDATA'] = os.path.join(_home, 'AppData', 'Roaming')
            env['LOCALAPPDATA'] = os.path.join(_home, 'AppData', 'Local')

        cmd = [
            buddy, '-p', '-y',
            '--permission-mode', 'bypassPermissions',
            '--allowedTools', 'Read,Edit,Write,Glob,Grep,Bash',
            '--max-turns', '30',
            '--add-dir', _project_root(),
            '--input-format', 'text',
            '--output-format', 'stream-json',
            '--include-partial-messages',
        ]
        if model:
            cmd += ['--model', model]

        try:
            proc = subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, cwd=_project_root(), env=env,
                creationflags=(CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0),
            )
        except Exception as e:
            return False, None, '启动 CLI 失败：%s' % e, 1

        with self._lock:
            self._procs[task_id] = proc

        watchdog = threading.Timer(_MAX_TASK_SECONDS, lambda: (self._cancel.__setitem__(task_id, True), self._terminate(proc)))
        watchdog.daemon = True
        watchdog.start()

        try:
            proc.stdin.write(prompt.encode('utf-8'))
            proc.stdin.close()
        except Exception:
            pass

        full = []
        result_text = None
        for raw_line in proc.stdout:
            if self._cancel.get(task_id):
                self._terminate(proc)
                break
            try:
                line = raw_line.decode('utf-8')
            except Exception:
                try:
                    line = raw_line.decode('gbk')
                except Exception:
                    line = raw_line.decode('utf-8', errors='replace')
            piece = line.rstrip('\n')
            if not piece:
                continue
            try:
                evt = json.loads(piece)
            except Exception:
                if piece:
                    full.append(piece)
                    self._emit(task_id, _sse_block('token', piece))
                continue
            delta_text = None
            delta_think = None
            if isinstance(evt, dict):
                t = evt.get('type')
                if t == 'stream_event' and isinstance(evt.get('event'), dict):
                    ev = evt['event']
                    if ev.get('type') == 'content_block_delta' and isinstance(ev.get('delta'), dict):
                        _d = ev['delta']
                        if _d.get('type') == 'thinking_delta':
                            delta_think = _d.get('thinking') or ''
                        elif _d.get('type') == 'text_delta':
                            delta_text = _d.get('text') or ''
                elif t == 'result' and isinstance(evt.get('result'), str):
                    result_text = evt.get('result')
            if delta_text:
                full.append(delta_text)
                self._emit(task_id, _sse_block('token', delta_text))
            if delta_think:
                self._emit(task_id, _sse_block('thinking', delta_think))

        try:
            proc.stdout.close()
        except Exception:
            pass
        err_text = ''
        try:
            err_text = proc.stderr.read().decode('utf-8', errors='replace') or ''
        except Exception:
            pass
        try:
            proc.stderr.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=30)
        except Exception:
            pass
        watchdog.cancel()

        reply = ''.join(full).strip()
        if not reply and result_text:
            reply = result_text.strip()
        if not reply and err_text and '认证' not in err_text and '未登录' not in err_text:
            reply = err_text.strip()
        return True, reply, err_text, proc.returncode


# 模块级单例
ai_mgr = AIChatManager()
_ai_mgr_singleton = ai_mgr
