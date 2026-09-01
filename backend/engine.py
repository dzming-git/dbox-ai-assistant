"""CodeBuddy对话引擎（归零版）。

设计原则：回到最原始的聊天体验——就像在 IDE 里和助手对话一样。
- 纯多轮文本对话，流式输出（SSE）。
- 底层经 CodeBuddy CLI（-p 非交互 + stream-json）调用，保留其文件读写/命令执行能力。
- 会话历史由服务端按 owner 维护，并落库到 sqlite（data/plugins/<key>/db/chat.db），
  重启/重载 extensions_host 后从历史库恢复，支持多会话、消息级 id 与 created_at。
- 不引入意图分类、阶段气泡、计划模式、反馈单建单、回滚、上下文续写等任何包裹逻辑。

运行于独立的拓展宿主进程（extensions_host），凭证经 host.vault 读取。
"""
import os
import re
import json
import time
import uuid
import queue
import threading
import subprocess
import logging

_logger = logging.getLogger('extensions_host.codebuddy')

try:
    from subprocess import CREATE_NEW_PROCESS_GROUP
except ImportError:
    CREATE_NEW_PROCESS_GROUP = 0

# 单个 AI 任务的最大执行时长（秒）。超时由看门狗强制结束进程树，防止子进程
# stdout 管道不关闭导致 worker 永久阻塞、队列卡死。
_MAX_TASK_SECONDS = 600

# 每轮对话回传给 CLI 的最大历史条数（用户+助手各算一条）。
_MAX_HISTORY = 20

# 会话标题自动取首条用户消息的前 N 个字符。
_TITLE_PREVIEW = 24

# 会话列表 preview 取最近一条消息的前 N 个字符。
_MSG_PREVIEW = 48

# 流式回复增量落盘的节流间隔（秒）。AI 回复只在生成完毕后写库的话，过程中
# 刷新页面（或进程重启/取消）会让已生成内容全丢；按此间隔把当前已生成的
# 部分写库，结束时再用最终正文更新同一条记录。
_STREAM_FLUSH_SEC = 2.0

# 极简系统提示：直接动手、中文说明，仅此而已。
_SYSTEM_PROMPT = (
    '你是嵌入在媒体库管理后台里的 CodeBuddy，拥有读写文件、运行命令的真实能力。\n'
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
        self._timed_out = {}                  # task_id -> True（看门狗超时置位）
        self._subscribers = {}                # task_id -> [queue.Queue, ...]（SSE 订阅者）
        self._buffers = {}                    # task_id -> [event_str, ...]（供重连续接）
        self._tasks = {}                      # task_id -> {owner_id, conversation_id, status, created_at}
        self._vault = None
        self._host = None
        self._db = None                       # sqlite 连接（历史落库，重启可恢复）
        self._db_path = None
        self._initialized = False
        # 会话级内存缓存（落库后的镜像，重启从 DB 重建）
        self._conv_meta = {}                  # cid -> {owner_id, title, created_at, updated_at}
        self._conv_msgs = {}                  # cid -> [ {id, role, text, created_at, task_id}, ... ]
        # 流式回复增量落盘的节流时间戳（task_id -> 上次写库时间）
        self._stream_flush_at = {}

    # ---------- 初始化 ----------
    def init(self, host, vault=None, tasks=None):
        self._host = host
        self._vault = vault or (getattr(host, 'vault', None) if host else None)
        # 历史落库：每个拓展在 data/plugins/<key>/db/chat.db 持有一份对话记录，
        # 重启/重载 extensions_host 后从历史库恢复，解决「刷新/重启即丢失」问题。
        if host is not None:
            try:
                self._db_path = host.db('chat')
            except Exception as e:
                _logger.error('CodeBuddy历史库路径获取失败: %s', e)
        with self._lock:
            if self._initialized:
                return
            self._initialized = True
        self._init_db()
        # 兼容历史库路径漂移：早期宿主按 manifest id=ai_assistant（下划线）落库，
        # 现宿主强制 key=文件夹名 ai-assistant（连字符）；且 data_dir root 在
        # DBOX_DATA_DIR 设置/未设置时分别落到 ProgramData 与项目根 data/。
        # 这导致同一份对话分散在多个 chat.db，重启后读到空库即表现为「记录消失」。
        # 启动时把所有分叉库合并进当前 canonical 库，保证历史不丢。
        self._merge_orphan_dbs()
        self._load_from_db()

    def _merge_orphan_dbs(self):
        """把历史上因 key 命名/数据目录漂移而分散的 chat.db 合并进当前 canonical 库。

        早期宿主按 manifest id=ai_assistant（下划线）落库，现强制 key=文件夹名
        ai-assistant（连字符）；且 DBOX_DATA_DIR 设置与否分别落到 ProgramData 与
        项目根 data/。这导致同一份对话分散在多个 chat.db，重启后读到空库即表现为
        「记录消失」。这里扫描所有可能的分叉库，把其中的会话与消息合并进当前库。
        """
        db = getattr(self, '_db', None)
        if not db or not getattr(self, '_db_path', None):
            return
        import glob as _glob
        cur_path = os.path.normcase(os.path.normpath(self._db_path))
        # 推导候选分叉库路径：
        # 1) 同 data_dir 下 key 的另一拼写（连字符 <-> 下划线）
        # 2) 另一 data_dir 根（ProgramData 与项目根 data/）下的同名库
        cands = set()
        for key_var in ('ai-assistant', 'ai_assistant'):
            # 当前 db 路径中以 key 片段替换
            parts = self._db_path.replace('/', os.sep).split(os.sep)
            for i, seg in enumerate(parts):
                if seg in ('ai-assistant', 'ai_assistant'):
                    alt = parts[:i] + [key_var] + parts[i + 1:]
                    cands.add(os.path.normcase(os.path.normpath(os.sep.join(alt))))
        # 也直接列举常见根
        for root in (r'C:\ProgramData\Dbox\data',
                     os.path.join(os.getcwd(), 'data'),
                     os.path.join(os.path.dirname(os.path.dirname(
                         os.path.dirname(os.path.dirname(self._db_path)))), 'data')):
            for key_var in ('ai-assistant', 'ai_assistant'):
                cands.add(os.path.normcase(os.path.normpath(
                    os.path.join(root, 'plugins', key_var, 'db', 'chat.db'))))
        cands.discard(cur_path)
        merged = 0
        for cand in cands:
            if not os.path.isfile(cand):
                continue
            try:
                import sqlite3 as _sql
                src = _sql.connect(cand, check_same_thread=False)
                src.row_factory = _sql.Row
                convs = src.execute(
                    'SELECT id, owner_id, title, created_at, updated_at '
                    'FROM conversations').fetchall()
                msgs = src.execute(
                    'SELECT conversation_id, owner_id, role, text, created_at, task_id '
                    'FROM messages ORDER BY id').fetchall()
                if not convs and not msgs:
                    src.close()
                    continue
                with self._lock:
                    for c in convs:
                        db.execute(
                            'INSERT OR IGNORE INTO conversations '
                            '(id, owner_id, title, created_at, updated_at) '
                            'VALUES (?,?,?,?,?)',
                            (c['id'], c['owner_id'], c['title'],
                             c['created_at'], c['updated_at']))
                    for m in msgs:
                        db.execute(
                            'INSERT INTO messages '
                            '(conversation_id, owner_id, role, text, created_at, task_id) '
                            'SELECT ?,?,?,?,?,? WHERE NOT EXISTS ('
                            'SELECT 1 FROM messages WHERE conversation_id=? '
                            'AND role=? AND text=? AND created_at=?)',
                            (m['conversation_id'], m['owner_id'], m['role'], m['text'],
                             m['created_at'], m['task_id'], m['conversation_id'],
                             m['role'], m['text'], m['created_at']))
                    db.commit()
                src.close()
                merged += 1
                _logger.info('CodeBuddy合并历史库成功: %s (%d 会话/%d 消息)',
                             cand, len(convs), len(msgs))
            except Exception as e:
                _logger.error('CodeBuddy合并历史库失败 %s: %s', cand, e)
        if merged:
            _logger.info('CodeBuddy共合并 %d 个历史分叉库', merged)
        self._start_worker()
        _logger.info('CodeBuddy引擎已初始化（归零版：纯多轮对话，会话级历史落库）')

    # ---------- 历史落库（sqlite）----------
    def _init_db(self):
        if not self._db_path:
            return
        try:
            import sqlite3
            self._db = sqlite3.connect(self._db_path, check_same_thread=False)
            self._db.execute(
                'CREATE TABLE IF NOT EXISTS conversations ('
                'id TEXT PRIMARY KEY, owner_id TEXT, title TEXT,'
                'created_at REAL, updated_at REAL)')
            self._db.execute(
                'CREATE TABLE IF NOT EXISTS messages ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, conversation_id TEXT,'
                'owner_id TEXT, role TEXT, text TEXT, created_at REAL,'
                'task_id TEXT)')
            self._db.commit()
        except Exception as e:
            _logger.error('CodeBuddy历史库初始化失败: %s', e)
            self._db = None

    def _load_from_db(self):
        if not getattr(self, '_db', None):
            return
        try:
            conv_rows = self._db.execute(
                'SELECT id, owner_id, title, created_at, updated_at '
                'FROM conversations').fetchall()
            msg_rows = self._db.execute(
                'SELECT id, conversation_id, owner_id, role, text, created_at, task_id '
                'FROM messages ORDER BY id').fetchall()
            with self._lock:
                for cid, owner_id, title, created_at, updated_at in conv_rows:
                    # owner_id 统一转字符串，避免与 JWT 的 int user_id 类型不一致
                    owner_id = None if owner_id is None else str(owner_id)
                    self._conv_meta[cid] = {
                        'owner_id': owner_id, 'title': title,
                        'created_at': created_at, 'updated_at': updated_at}
                    self._conv_msgs.setdefault(cid, [])
                for mid, cid, owner_id, role, text, created_at, task_id in msg_rows:
                    if cid not in self._conv_meta:
                        # 孤儿消息：会话元数据缺失时补一个占位会话，保证消息不丢
                        self._conv_meta[cid] = {
                            'owner_id': None if owner_id is None else str(owner_id),
                            'title': (text or '')[:_TITLE_PREVIEW],
                            'created_at': created_at or time.time(),
                            'updated_at': created_at or time.time()}
                        self._conv_msgs.setdefault(cid, [])
                    self._conv_msgs[cid].append({
                        'id': mid, 'role': role, 'text': text,
                        'created_at': created_at, 'task_id': task_id})
        except Exception as e:
            _logger.error('CodeBuddy历史载入失败: %s', e)

    def _insert_conv(self, cid, owner_id, title, created_at, updated_at):
        db = getattr(self, '_db', None)
        if not db:
            return
        try:
            with self._lock:
                db.execute(
                    'INSERT OR REPLACE INTO conversations'
                    '(id, owner_id, title, created_at, updated_at) '
                    'VALUES (?,?,?,?,?)',
                    (cid, owner_id, title, created_at, updated_at))
                db.commit()
        except Exception as e:
            _logger.error('CodeBuddy会话写入失败: %s', e)

    def _append_db(self, cid, owner_id, role, text, created_at, task_id):
        db = getattr(self, '_db', None)
        if not db:
            return
        try:
            with self._lock:
                cur = db.execute(
                    'INSERT INTO messages'
                    '(conversation_id, owner_id, role, text, created_at, task_id) '
                    'VALUES (?,?,?,?,?,?)',
                    (cid, owner_id, role, text, created_at, task_id))
                db.commit()
                mid = cur.lastrowid
            return mid
        except Exception as e:
            _logger.error('CodeBuddy历史写入失败: %s', e)
            return None

    def _sync_stream_msg(self, cid, mid, text, created_at, task_id):
        """把流式中的助手消息同步进内存 _conv_msgs（已存在则更新，避免重复插入）。"""
        msgs = self._conv_msgs.setdefault(cid, [])
        for m in msgs:
            if m.get('task_id') == task_id and m.get('role') == 'assistant':
                m['text'] = text
                m['created_at'] = created_at
                if mid is not None:
                    m['id'] = mid
                return
        msgs.append({'id': mid, 'role': 'assistant', 'text': text,
                     'created_at': created_at, 'task_id': task_id})

    def _upsert_assistant_msg(self, cid, owner_id, text, created_at, task_id):
        """流式回复增量落盘：按 task_id 找该任务已写入的助手消息，有则更新，无则插入。

        原实现只在整条回复生成完毕后才写库，流式过程中刷新页面（或进程重启、
        取消、异常）会让已生成的内容全部丢失。这里让流式每间隔一段时间就把
        当前已生成的部分写库，结束时再用最终正文更新同一条记录。
        """
        db = getattr(self, '_db', None)
        if not db:
            return None
        try:
            with self._lock:
                row = db.execute(
                    'SELECT id FROM messages WHERE task_id=? AND role=? '
                    'ORDER BY id LIMIT 1', (task_id, 'assistant')).fetchone()
                if row:
                    db.execute('UPDATE messages SET text=?, created_at=? WHERE id=?',
                               (text, created_at, row[0]))
                    db.commit()
                    mid = row[0]
                else:
                    cur = db.execute(
                        'INSERT INTO messages'
                        '(conversation_id, owner_id, role, text, created_at, task_id) '
                        'VALUES (?,?,?,?,?,?)',
                        (cid, owner_id, 'assistant', text, created_at, task_id))
                    db.commit()
                    mid = cur.lastrowid
                self._sync_stream_msg(cid, mid, text, created_at, task_id)
            return mid
        except Exception as e:
            _logger.error('CodeBuddy流式消息写入失败: %s', e)
            return None

    def _update_conv_time(self, cid, updated_at):
        db = getattr(self, '_db', None)
        if not db:
            return
        try:
            with self._lock:
                db.execute(
                    'UPDATE conversations SET updated_at=? WHERE id=?',
                    (updated_at, cid))
                db.commit()
        except Exception as e:
            _logger.error('CodeBuddy会话时间更新失败: %s', e)

    def _db_delete_conv(self, cid):
        db = getattr(self, '_db', None)
        if not db:
            return
        try:
            with self._lock:
                db.execute('DELETE FROM messages WHERE conversation_id=?', (cid,))
                db.execute('DELETE FROM conversations WHERE id=?', (cid,))
                db.commit()
        except Exception as e:
            _logger.error('CodeBuddy会话删除失败: %s', e)

    def _db_clear_conv(self, cid):
        db = getattr(self, '_db', None)
        if not db:
            return
        try:
            with self._lock:
                db.execute('DELETE FROM messages WHERE conversation_id=?', (cid,))
                db.commit()
        except Exception as e:
            _logger.error('CodeBuddy会话清空失败: %s', e)

    def _start_worker(self):
        if self._worker and self._worker.is_alive():
            return
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()

    # ---------- 会话管理（内存 + 落库）----------
    def _owner_convs(self, owner_id):
        """返回该用户会话 id 列表（按 updated_at 倒序）。

        owner_id 统一按字符串比较：JWT 的 user_id 是 int（如 1），而历史库
        里存的是 str（'1'），若直接 `==` 会因类型不一致过滤掉全部历史，
        表现为「重启后聊天记录丢失」。这里两侧都转 str 归一化。
        """
        def _norm(v):
            return None if v is None else str(v)
        owner_str = _norm(owner_id)
        cids = [c for c, m in self._conv_meta.items()
                if _norm(m.get('owner_id')) == owner_str]
        cids.sort(key=lambda c: self._conv_meta[c].get('updated_at', 0), reverse=True)
        return cids

    def _ensure_default_conv(self, owner_id):
        """确保用户至少有一个会话（首条消息前自动建好）。返回 cid。"""
        with self._lock:
            cids = self._owner_convs(owner_id)
            if cids:
                return cids[0]
            cid = uuid.uuid4().hex
            now = time.time()
            self._conv_meta[cid] = {
                'owner_id': owner_id, 'title': '新对话',
                'created_at': now, 'updated_at': now}
            self._conv_msgs[cid] = []
            self._insert_conv(cid, owner_id, '新对话', now, now)
            return cid

    def list_conversations(self, owner_id):
        with self._lock:
            out = []
            for cid in self._owner_convs(owner_id):
                meta = self._conv_meta[cid]
                msgs = self._conv_msgs.get(cid, [])
                preview = ''
                if msgs:
                    preview = msgs[-1].get('text') or ''
                    preview = preview.replace('\n', ' ').strip()[:_MSG_PREVIEW]
                out.append({
                    'id': cid,
                    'title': meta.get('title') or '新对话',
                    'created_at': meta.get('created_at'),
                    'updated_at': meta.get('updated_at'),
                    'preview': preview,
                    'msg_count': len(msgs),
                })
        return out

    def create_conversation(self, owner_id, title=None):
        # owner_id 必须统一转 str：chat() 入队时会把 owner_id str() 后再与
        # _conv_meta 里的值比较。此处若原样存 int（如 JWT 的 1），新建出来的会话
        # 在首次发消息时就会因 1 != '1' 被判成「会话不存在」。
        owner_id = None if owner_id is None else str(owner_id)
        with self._lock:
            cid = uuid.uuid4().hex
            now = time.time()
            title = (title or '').strip() or '新对话'
            self._conv_meta[cid] = {
                'owner_id': owner_id, 'title': title,
                'created_at': now, 'updated_at': now}
            self._conv_msgs[cid] = []
            self._insert_conv(cid, owner_id, title, now, now)
            return cid

    def delete_conversation(self, owner_id, cid):
        with self._lock:
            meta = self._conv_meta.get(cid)
            if not meta or str(meta.get('owner_id')) != str(owner_id):
                return False
            self._conv_meta.pop(cid, None)
            self._conv_msgs.pop(cid, None)
            self._db_delete_conv(cid)
            return True

    def clear_conversation(self, owner_id, cid):
        with self._lock:
            meta = self._conv_meta.get(cid)
            if not meta or str(meta.get('owner_id')) != str(owner_id):
                return False
            self._conv_msgs[cid] = []
            self._db_clear_conv(cid)
            return True

    def rename_conversation(self, owner_id, cid, title):
        with self._lock:
            meta = self._conv_meta.get(cid)
            if not meta or str(meta.get('owner_id')) != str(owner_id):
                return False
            title = (title or '').strip() or '新对话'
            meta['title'] = title
            meta['updated_at'] = time.time()
            self._insert_conv(cid, owner_id, title, meta['created_at'], meta['updated_at'])
            return True

    # ---------- 历史（按会话）----------
    def history(self, owner_id, conversation_id=None, limit=50):
        with self._lock:
            if conversation_id:
                msgs = list(self._conv_msgs.get(conversation_id, []))
            else:
                cids = self._owner_convs(owner_id)
                cid = cids[0] if cids else None
                msgs = list(self._conv_msgs.get(cid, [])) if cid else []
        return msgs[-limit:]

    def clear(self, owner_id=None, conversation_id=None):
        with self._lock:
            if conversation_id:
                return self.clear_conversation(owner_id, conversation_id)
            if owner_id is None:
                # 全清（仅清空当前用户的全部会话），逐个删，保持 DB 一致
                for cid in list(self._owner_convs(owner_id)):
                    self._conv_meta.pop(cid, None)
                    self._conv_msgs.pop(cid, None)
                    self._db_delete_conv(cid)
                return True
            for cid in list(self._owner_convs(owner_id)):
                self._conv_meta.pop(cid, None)
                self._conv_msgs.pop(cid, None)
                self._db_delete_conv(cid)
            return True

    # ---------- 入队 ----------
    def chat(self, message, owner_id=None, model=None, conversation_id=None):
        """入队一条用户消息，返回 (task_id, err)。会在指定/默认会话里追加该用户消息。"""
        message = (message or '').strip()
        if not message:
            return None, '消息为空'
        # 统一 owner_id 为字符串，与历史库存储（str）保持一致，避免 int/str 类型
        # 不匹配导致会话归属判断与历史查询把记录过滤掉（表现为「记录丢失」）。
        owner_id = None if owner_id is None else str(owner_id)
        with self._lock:
            # 解析目标会话：显式指定则校验归属；否则取最近会话，无则建默认
            if conversation_id:
                meta = self._conv_meta.get(conversation_id)
                # 两侧都转 str 再比，与 delete/rename/clear 保持一致：
                # 历史库存的是 str 而 JWT 的 user_id 是 int，严格 == 会因类型
                # 不一致把会话判成不存在（表现为「新建对话后聊天报错」）。
                if not meta or str(meta.get('owner_id')) != str(owner_id):
                    return None, '会话不存在'
                cid = conversation_id
            else:
                cid = self._ensure_default_conv(owner_id)
            task_id = uuid.uuid4().hex
            now = time.time()
            self._tasks[task_id] = {
                'owner_id': owner_id, 'conversation_id': cid,
                'status': self.STATUS_PENDING, 'created_at': now, 'model': model,
            }
            self._subscribers[task_id] = []
            self._buffers[task_id] = []
            # 追加用户消息（内存 + 落库），首条消息自动作为会话标题
            meta = self._conv_meta[cid]
            if not meta.get('title') or meta.get('title') == '新对话':
                meta['title'] = message[:_TITLE_PREVIEW]
                self._insert_conv(cid, owner_id, meta['title'],
                                  meta['created_at'], now)
            meta['updated_at'] = now
            self._update_conv_time(cid, now)
            msg = {'id': None, 'role': 'user', 'text': message,
                   'created_at': now, 'task_id': task_id}
            mid = self._append_db(cid, owner_id, 'user', message, now, task_id)
            if mid is not None:
                msg['id'] = mid
            self._conv_msgs.setdefault(cid, []).append(msg)
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
        # 先 yield 一个 SSE 注释块，确保响应头尽早发出（Flask 流式响应在
        # 生成器首次 yield 时才 flush 响应头；若首事件迟迟未产生，部分客户端
        # /代理会一直等待响应头而表现为“连接中断”。注释行对前端 EventSource 透明）。
        yield ': connected\n\n'
        # 先回放已产生的缓冲（重连续接）
        for blk in buf:
            yield blk

        # 任务已结束（如刷新后回放续传）：缓冲回放完即收尾，避免连接长期挂起
        with self._lock:
            _st = self._tasks.get(task_id, {}).get('status')
        if _st in (self.STATUS_COMPLETED, self.STATUS_FAILED, self.STATUS_CANCELLED):
            with self._lock:
                _subs = self._subscribers.get(task_id, [])
                if sub_q in _subs:
                    _subs.remove(sub_q)
            yield _sse_block('done', 'closed')
            return
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

    def _close_subscribers(self, task_id):
        """任务结束时向所有 SSE 订阅者推送 None 哨兵，使订阅生成器正常退出，
        避免连接长期挂起 / 重连造成线程与队列泄漏。"""
        with self._lock:
            subs = self._subscribers.pop(task_id, [])
        for q in subs:
            try:
                q.put(None)
            except Exception:
                pass

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
        """返回该用户任务概览，供宿主侧轻量轮询判断是否忙碌/有未读。"""
        with self._lock:
            owned = [(tid, t) for tid, t in self._tasks.items()
                     if str(t.get('owner_id')) == str(owner_id)]
            active = any(t['status'] == self.STATUS_RUNNING for _, t in owned)
            pending = [tid for tid, t in owned if t['status'] == self.STATUS_PENDING]
            # 进行中的任务：供前端刷新页面后恢复订阅。订阅时后端会回放已生成的
            # 内容（_buffers），因此刷新不会丢回答；前端据此跳过历史里「进行中」
            # 的助手消息，避免与回放内容重复渲染（表现为文字错行/重复）。
            running = [
                {'id': tid, 'status': t['status'],
                 'conversation_id': t.get('conversation_id')}
                for tid, t in owned if t['status'] == self.STATUS_RUNNING
            ]
            history = [
                {'id': tid, 'status': t['status'], 'conversation_id': t.get('conversation_id')}
                for tid, t in owned
                if t['status'] in (self.STATUS_COMPLETED, self.STATUS_FAILED)
            ]
        return {'active': active, 'pending': pending,
                'running': running, 'history': history}

    def is_busy(self):
        """是否存在正在执行或排队中的任务（供宿主热重载保护使用）。

        热重载会重新 import 整个插件模块，导致当前 ai_mgr 单例及其手上的
        子进程 / SSE 订阅者被丢弃，正在跑的生成任务会断流。故 reload 前宿主
        会征询此钩子，返回 True 时延迟重载直至空闲。
        """
        with self._lock:
            for t in self._tasks.values():
                if t.get('status') in (self.STATUS_RUNNING, self.STATUS_PENDING):
                    return True
        return False

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
                    self._timed_out.pop(task_id, None)
                    self._cancel.pop(task_id, None)
                    self._stream_flush_at.pop(task_id, None)
                self._emit(task_id, _sse_block('done', 'closed'))
                self._close_subscribers(task_id)

    def _run_task(self, task_id):
        with self._lock:
            info = self._tasks.get(task_id, {})
            owner_id = info.get('owner_id')
            cid = info.get('conversation_id')
            model = info.get('model')
            info['status'] = self.STATUS_RUNNING
        # 构造 prompt：system + 最近若干轮历史 + 当前用户消息
        with self._lock:
            h = list(self._conv_msgs.get(cid, [])) if cid else []
        recent = h[-(_MAX_HISTORY * 2):] if _MAX_HISTORY else h
        parts = [_SYSTEM_PROMPT, '']
        for turn in recent:
            role = '用户' if turn['role'] == 'user' else '助手'
            parts.append('%s：%s' % (role, turn['text']))
        prompt = '\n'.join(parts)

        self._emit(task_id, _sse_block('queue', '0'))

        ok, reply, err, rc, timed_out, result_received = self._call_cli(task_id, prompt, model)
        # 异常终止判定：CLI 非 0 退出、未收到 result 终态事件（中途崩溃/被杀）、
        # 超时或被取消——这些都意味着回答不完整，绝不能再静默存成「已完成」。
        abnormal = (not ok) or timed_out or (rc not in (0, None)) or (not result_received)
        if abnormal:
            self._finish_failed(task_id, cid, owner_id, reply, err, timed_out, rc,
                                 result_received, not ok)
            return

        final = (reply or '').strip()
        if not final:
            final = '（助手未返回内容）'
        # 把助手回复追加进会话历史（仅最终正文，不含思考过程），并落库
        with self._lock:
            now = time.time()
            meta = self._conv_meta.get(cid)
            if meta:
                meta['updated_at'] = now
                self._update_conv_time(cid, now)
            # 用最终正文更新同一条记录（流式期间已增量写过则 UPDATE，否则 INSERT）
            mid = self._upsert_assistant_msg(cid, owner_id, final, now, task_id)
            if mid is None:
                # 落库不可用（无 DB/异常）时至少保住内存态
                self._sync_stream_msg(cid, None, final, now, task_id)
            self._tasks[task_id]['status'] = self.STATUS_COMPLETED
        self._emit(task_id, _sse_block('assistant', final))

    def _finish_failed(self, task_id, cid, owner_id, partial, err, timed_out, rc,
                        result_received=False, cli_error=False):
        """失败/超时/异常终止收尾：保留已生成的部分内容（避免进度全丢），并明确
        告知用户被截断（含原因），状态置为 cancelled/failed，而非静默 COMPLETED。"""
        text = (partial or '').strip()
        with self._lock:
            now = time.time()
            meta = self._conv_meta.get(cid)
            if meta:
                meta['updated_at'] = now
                self._update_conv_time(cid, now)
            if text:
                self._upsert_assistant_msg(cid, owner_id, text, now, task_id)
            self._tasks[task_id]['status'] = (
                self.STATUS_CANCELLED if timed_out else self.STATUS_FAILED)
        if text:
            self._emit(task_id, _sse_block('assistant', text))
        # 构造明确的截断原因，让用户知道这是「中断」而非「正常短回答」
        if timed_out:
            reason = ('执行超时（已超过 %d 秒）已自动终止，以上为已生成的部分内容'
                      % _MAX_TASK_SECONDS)
        elif rc not in (0, None):
            reason = '生成中断（CodeBuddy CLI 异常退出，退出码 %s），以上为已生成的部分内容' % rc
        elif not result_received:
            reason = '生成中断（未收到完整结果，可能 CLI 已崩溃/被杀），以上为已生成的部分内容'
        elif err:
            reason = err
        else:
            reason = '已停止'
        self._emit(task_id, _sse_block('error', reason))

    def _call_cli(self, task_id, prompt, model):
        """调用 CodeBuddy CLI（-p + stream-json），把 text_delta/thinking_delta 流式推送。

        关键修复：stderr 用独立线程并发排空。Windows 上子进程 stderr 管道缓冲很小，
        若 CLI 向 stderr 写入较多内容（进度/诊断，甚至最终回复）而父进程不及时读取，
        stderr 写满后会阻塞子进程、进而卡死 stdout 读取——表现为「一直思考、不动」，
        直到看门狗 600s 强杀，留下被截断的回答。并发排空可彻底消除该死锁。
        """
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
            self._timed_out.pop(task_id, None)

        # 并发排空 stderr：见 _call_cli 文档说明，避免 Windows 下 stderr 管道写满
        # 导致子进程阻塞、stdout 卡死的死锁。
        _err_chunks = []
        def _drain_stderr():
            try:
                for _l in proc.stderr:
                    _err_chunks.append(_l.decode('utf-8', 'replace'))
            except Exception:
                pass
        _stderr_t = threading.Thread(target=_drain_stderr, daemon=True)
        _stderr_t.start()

        watchdog = threading.Timer(
            _MAX_TASK_SECONDS,
            lambda: (self._timed_out.__setitem__(task_id, True),
                     self._cancel.__setitem__(task_id, True),
                     self._terminate(proc)))
        watchdog.daemon = True
        watchdog.start()

        try:
            proc.stdin.write(prompt.encode('utf-8'))
            proc.stdin.close()
        except Exception:
            pass

        full = []
        result_text = None
        result_received = False  # 是否收到 CLI 的 result 终态事件（用于区分「正常完成」与「中途崩溃/被杀」）
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
                    result_received = True
            if delta_text:
                full.append(delta_text)
                self._emit(task_id, _sse_block('token', delta_text))
                # 流式增量落盘（节流）：让刷新页面/重启后仍能看到已生成的部分
                now_t = time.time()
                if now_t - self._stream_flush_at.get(task_id, 0) >= _STREAM_FLUSH_SEC:
                    self._stream_flush_at[task_id] = now_t
                    _info = self._tasks.get(task_id, {})
                    self._upsert_assistant_msg(
                        _info.get('conversation_id'), _info.get('owner_id'),
                        ''.join(full).strip(), now_t, task_id)
            if delta_think:
                self._emit(task_id, _sse_block('thinking', delta_think))

        try:
            proc.stdout.close()
        except Exception:
            pass
        # 等 stderr 排空线程结束（带超时保护，避免卡在 join）
        _stderr_t.join(timeout=5)
        err_text = ''.join(_err_chunks) or ''
        try:
            proc.stderr.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=30)
        except Exception:
            pass
        watchdog.cancel()

        timed_out = self._timed_out.pop(task_id, False)
        reply = ''.join(full).strip()
        if not reply and result_text:
            reply = result_text.strip()
        if not reply and err_text and '认证' not in err_text and '未登录' not in err_text:
            reply = err_text.strip()
        return True, reply, err_text, proc.returncode, timed_out, result_received


# 模块级单例
ai_mgr = AIChatManager()
_ai_mgr_singleton = ai_mgr
