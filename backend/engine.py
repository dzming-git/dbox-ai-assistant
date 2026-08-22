"""AI 助手对话队列管理器

将 AI 助手对话改造为「底层排队 + UI 无状态」模型：

- 用户发送的消息以任务（task）形式入队，后端立即返回 task_id，不阻塞；
- 底层用 FIFO 队列堆积「未处理」任务，单 worker 串行执行 CodeBuddy CLI；
- 任务状态机：pending（排队中）-> running（正在处理）-> completed / failed / cancelled；
- 对话上下文（多轮）由服务端持久化，前端不持有历史，仅做渲染与下发；
- 已处理任务保留为历史队列，列表接口默认返回最近 N 条，前端可「展开更多」翻页；
- 任意任务可经 task_id 订阅 SSE，支持多端同时订阅与刷新后重连（任务仍在执行时续接流式输出）。

数据落在 data/ai_assistant.db（独立表 ai_assistant_tasks），并轻量镜像到统一任务表（kind='ai_assistant'），
使全局任务管理器也能看到正在处理的 AI 任务。

本模块运行于独立的拓展宿主进程（extensions_host），不直接 import 主服务的业务模块：
- 反馈中心的建单经由 platform_client 以 HTTP 转发给主服务的内部接口完成；
- codebuddy 凭证经由共享库 shared.credential_vault 读取（中立、无业务依赖）。
"""
import os
import sys
import re
import json
import time
import uuid
import signal
import queue
import threading
import subprocess
import sqlite3

import logging
_logger = logging.getLogger('extensions_host.ai_assistant')

# Plan 模式：计划文档（md）管理器（同目录，worker 线程直接复用）
try:
    import plan_manager
except Exception:
    plan_manager = None

try:
    from subprocess import CREATE_NEW_PROCESS_GROUP
except ImportError:
    CREATE_NEW_PROCESS_GROUP = 0

# 单个 AI 任务的最大执行时长（秒）。超时则由看门狗强制结束进程树，
# 防止 CodeBuddy 拉起子进程后 stdout 管道不关闭导致 worker 永久阻塞、队列卡死。
_MAX_TASK_SECONDS = 600

# AI 助手可选模型列表（与 codebuddy CLI --model 支持的 ID 保持一致）。
# 每个条目 dict：{ id, name }。修改后需与 codebuddy --help 输出的模型清单对齐。
AI_MODELS = [
    {'id': 'hy3', 'name': 'Hybrid 3'},
    {'id': 'deepseek-v4-pro', 'name': 'DeepSeek V4 Pro'},
    {'id': 'deepseek-v4-flash', 'name': 'DeepSeek V4 Flash'},
    {'id': 'glm-5.3', 'name': 'GLM 5.3'},
    {'id': 'glm-5.2', 'name': 'GLM 5.2'},
    {'id': 'glm-5.1', 'name': 'GLM 5.1'},
    {'id': 'glm-5v-turbo', 'name': 'GLM 5V Turbo'},
    {'id': 'minimax-m3-pay', 'name': 'MiniMax M3 Pay'},
    {'id': 'minimax-m2.7', 'name': 'MiniMax M2.7'},
    {'id': 'kimi-k3-2', 'name': 'Kimi K3.2'},
    {'id': 'kimi-k2.7', 'name': 'Kimi K2.7'},
    {'id': 'kimi-k2.6', 'name': 'Kimi K2.6'},
]


def list_models():
    """返回可选模型列表，供前端渲染下拉选择。"""
    return list(AI_MODELS)

# ----------------------------------------------------------------------------
# CLI 辅助函数（原 routes.py 中的 AI 专用逻辑迁移至此）
# ----------------------------------------------------------------------------
_ANTHROPIC_API_KEY_ENV = 'ANTHROPIC_API_KEY'
_CODEBUDDY_API_KEY_ENV = 'CODEBUDDY_API_KEY'
_CODEBUDDY_ENV_ENV = 'CODEBUDDY_INTERNET_ENVIRONMENT'
_CODEBUDDY_TOKEN_DOMAIN = 'codebuddy'
# 中国版网络环境（与 CLI 输出 apiKeySource: copilot.tencent.com 一致）
_CODEBUDDY_INTERNET_ENVIRONMENT = 'internal'


def _load_codebuddy_token() -> str:
    """从通用凭证保险库读取 codebuddy token（与 feedback_ai 一致）。

    CodeBuddy CLI 在 -p 非交互模式下始终使用 CODEBUDDY_API_KEY 鉴权，
    故优先读取该环境变量作为兜底。
    """
    for env_name in (_CODEBUDDY_API_KEY_ENV, _ANTHROPIC_API_KEY_ENV):
        env_token = os.environ.get(env_name)
        if env_token:
            return env_token.strip()
    # 纯插件模式：优先使用宿主注入的 vault（host.vault），不 import 框架内部模块
    vault = getattr(_ai_mgr_singleton, '_vault', None)
    if vault is not None:
        tok = vault.get(_CODEBUDDY_TOKEN_DOMAIN)
        if tok:
            return tok.strip()
        try:
            for rec in vault.list_all():
                if rec.get('kind') == 'token' and 'codebuddy' in (rec.get('name') or '').lower():
                    return (rec.get('value') or '').strip()
        except Exception:
            pass
    # 回退：直接读取凭证保险库（兼容非插件旧路径）
    try:
        from shared.credential_vault import CredentialVault, data_dir_for
        vault = CredentialVault(data_dir_for())
        tok = vault.get_token(domain=_CODEBUDDY_TOKEN_DOMAIN)
        if tok:
            return tok.strip()
        for rec in vault.list_all():
            if rec.get('kind') == 'token' and 'codebuddy' in (rec.get('name') or '').lower():
                return (rec.get('value') or '').strip()
    except Exception:
        pass
    return ''


def _project_root() -> str:
    """定位 dbox 项目根目录。

    引擎文件历史上位于 src/extensions_host/，后迁移为纯插件（自包含于
    extensions/ai_assistant/backend/）。为避免按相对层级硬编码导致的越迁
    错位（少/多上一级都会让 git / --add-dir 指向错误目录），改为**向上查找
    首个含 .git 的目录**作为项目根——该插件目录本身处于 dbox 仓库内，故能
    稳定命中 dbox 根。
    """
    d = os.path.dirname(os.path.abspath(__file__))
    # 优先用环境变量（若宿主显式指定了仓库根）
    env_root = os.environ.get('DBOX_PROJECT_ROOT') or os.environ.get('DBOX_REPO_ROOT')
    if env_root and os.path.isdir(os.path.join(env_root, '.git')):
        return env_root
    while True:
        if os.path.isdir(os.path.join(d, '.git')):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            # 走到盘符仍未找到 .git，回退到相对估算（backend 上四级）
            return os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))))))
        d = parent


def _resolve_buddy_cli() -> str:
    """定位 codebuddy CLI 绝对路径。

    服务可能以不同用户（如 LocalSystem）运行，%APPDATA% 解析到的目录
    并不含 npm，故需在常见位置逐一回退；找不到时再尝试 PATH 搜索。
    """
    cands = []
    env_buddy = os.environ.get('DBOX_BUDDYCN')
    if env_buddy:
        cands.append(env_buddy)
    appdata = os.environ.get('APPDATA')
    if appdata:
        cands.append(os.path.join(appdata, 'npm', 'codebuddy.cmd'))
    # 常见用户绝对路径（与本项目实际运行用户一致）
    for uname in ('71555',):
        cands.append(r'C:\Users\%s\AppData\Roaming\npm\codebuddy.cmd' % uname)
        cands.append(r'C:\Users\%s\AppData\Local\npm\codebuddy.cmd' % uname)
    # 项目内的 codebuddy（若在 PATH 或本地）
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
    """返回存放 CodeBuddy 登录会话的交互用户家目录。

    主服务可能以 SYSTEM/服务账户运行，其本地登录会话位于交互用户（如 71555）
    的 ~/.codebuddy 下。优先用环境变量 DBOX_BUDDYCN_HOME 指定，否则回退到
    硬编码的常见用户名家目录；找不到则返回空串（沿用调用方环境）。
    """
    env_home = os.environ.get('DBOX_BUDDYCN_HOME')
    if env_home and os.path.isdir(env_home):
        return env_home
    for uname in ('71555',):
        home = r'C:\Users\%s' % uname
        if os.path.isdir(home):
            return home
    return ''


def _is_auth_error(text: str) -> bool:
    t = (text or '').lower()
    return any(k in t for k in ('未登录', '认证失败', 'auth fail', 'unauthorized',
                                'invalid api key', 'login required', 'please login'))


def _build_reply(out_lines, err_text, returncode):
    """从 stdout 行与 stderr 文本构造最终回复，返回 (reply, fell_back_stdout)。

    - stdout 有内容时优先采用；
    - stdout 为空、退出码正常、且 stderr 承载了助手正文时，回退采用 stderr，
      避免聊天框出现「（任务已执行完成，无文本输出）」占位。buddy 在部分运行
      环境（非 TTY 管道）下会把最终回复写到 stderr 而非 stdout，此前只读 stdout
      导致正文被丢弃、频繁出现空输出。
    - 认证错误 / 崩溃栈已由调用方在 returncode 非 0 或 _is_auth_error 时拦截，
      此处 stderr 内容即助手正文，可直接采用。
    """
    reply = '\n'.join(out_lines or []).strip()
    if reply:
        return reply, False
    if returncode in (0, None) and err_text and not _is_auth_error(err_text):
        err_reply = err_text.strip()
        if err_reply:
            return err_reply, True
    return '', False


# 宿主在 _build_prompt 里把「意图提示 / 阶段指令」等控制备注与用户问题拼成同一段
# 纯文本（buddy CLI 在 --input-format text 下无 system 角色隔离），模型可能把这些
# 仅供内部判断的备注当作对话内容回显进回复。此处统一在捕获后剥离，彻底阻断其进入
# 气泡、存库与跨阶段传播。
_CONTROL_LINE_RE = re.compile(
    r'^\s*（系统初步判定本条用户意图为：[^）]*）\s*$'
    r'|^\s*（本条为「继续」意图：.*?）\s*$'
    r'|^\s*【本阶段任务：.*?】.*$'
)


def _strip_control_lines(text):
    """剔除回显进回复的宿主控制备注行，返回清理后的正文（可能为空）。"""
    if not text:
        return ''
    out = []
    for line in text.split('\n'):
        if _CONTROL_LINE_RE.match(line):
            continue
        out.append(line)
    return '\n'.join(out).strip()


# AI 某阶段确实未返回任何有效内容时使用的占位提示（区分于「任务完成」）：
# 旧文案「（任务已执行完成，无文本输出）」会把「空输出」误报成「完成」，已替换为
# 更准确的表述，避免误导用户以为任务已正常收尾。
_PLACEHOLDER_AI_EMPTY = '（AI 未返回有效内容，请重试或查看运行日志）'


def _is_placeholder_text(text):
    """判定文本是否仅为空输出占位提示（任何版本），供续写/回复逻辑跳过它。"""
    return bool(text) and (
        text.startswith('（任务已执行完成')
        or text.startswith(_PLACEHOLDER_AI_EMPTY[:6])
    )


def _sse_block(event: str, data) -> str:
    """构造一段合规的 SSE 文本块。

    data 中的换行会被拆成多行 `data:` 字段，避免破坏 SSE 协议
    （否则含换行的回复会导致事件解析错位、前端拿不到完整回复）。
    """
    data = '' if data is None else str(data)
    lines = data.split('\n')
    return 'event: %s\n' % event + ''.join('data: ' + ln + '\n' for ln in lines) + '\n'


def _parse_phases(raw):
    """把存库的 phases JSON 字符串解析为阶段列表；缺失/损坏时回退空列表。"""
    if not raw:
        return None
    try:
        v = json.loads(raw)
        return v if isinstance(v, list) else None
    except Exception:
        return None


def _file_feedback(ftype: str, title: str, content: str, extra: dict = None,
                  status: str = 'open', comment: str = None, comments: list = None):
    """在反馈中心建一条反馈单，返回新单号；失败返回 None。

    仅由 _maybe_file_feedback / _maybe_create_tracking_ticket 调用。
    身份遵循项目准则：反馈中心交互使用「自动助手」身份
    （submitter='自动助手'、source='ai_assistant'、auto_classified=True）。
    建单经 platform_client 转发给主服务的内部接口 /internal/feedback 完成，
    使本模块无需直接依赖主服务的 backend.feedback_db。
    extra / status 用于 AI 处理完成后的「跟踪单」（关联提交哈希、置待验证）。
    """
    try:
        from platform_client import file_feedback
        if ftype not in ('bug', 'suggestion', 'other'):
            ftype = 'suggestion'
        title = (title or '').strip()
        content = (content or '').strip()
        if not title and not content:
            return None
        return file_feedback(ftype, title, content, extra=extra, status=status,
                             comment=comment, comments=comments)
    except Exception as e:
        try:
            import logging
            logging.getLogger('extensions_host').warning('AI 助手建单失败: %s' % e)
        except Exception:
            pass
        return None


def _maybe_file_feedback(reply: str):
    """若 AI 回复内含 feedback-request 块，则建单并回填单号、剥离该块。

    返回 (处理后的回复文本, 新建单号或 None)。解析/建单失败时保留原回复、仅剥离块，
    单号返回 None。
    """
    if not reply:
        return reply, None
    m = _FB_RE.search(reply)
    if not m:
        return reply, None
    issue_id = None
    try:
        data = json.loads(m.group(1).strip())
        ftype = data.get('type')
        title = data.get('title')
        content = data.get('content')
        if isinstance(ftype, str) and isinstance(title, str) and isinstance(content, str):
            issue_id = _file_feedback(ftype, title, content)
    except Exception:
        issue_id = None
    # 剥离 feedback-request 围栏块（避免在前端露出原始 JSON）
    reply = (reply[:m.start()] + reply[m.end():]).strip()
    if issue_id:
        token = '#(待分配)'
        if token in reply:
            reply = reply.replace(token, '#' + issue_id, 1)
        else:
            reply = reply + ('\n\n📋 已提交反馈单：#%s' % issue_id)
    return reply, issue_id


def _git_rev_head(repo: str) -> str:
    """返回仓库当前 HEAD 提交哈希；非 git 仓库或出错返回空串。"""
    try:
        out = subprocess.run(
            ['git', 'rev-parse', 'HEAD'], cwd=repo,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=20,
        ).stdout.decode('utf-8', errors='replace').strip()
        return out
    except Exception:
        return ''


def _git_status_porcelain(repo: str):
    """返回 `git status --porcelain` 的解码输出；非 git 仓库 / 出错返回 None。"""
    try:
        out = subprocess.run(
            ['git', 'status', '--porcelain'], cwd=repo,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=20,
        ).stdout.decode('utf-8', errors='replace')
        return out
    except Exception:
        return None


def _git_dirty_files(repo: str):
    """返回当前工作树中「未提交」文件集合（含未跟踪与已修改/已暂存）。

    用于做事后比对：只有「基线集合之外的新增脏文件」才可归因于本次 AI 运行。
    非 git 仓库或无法判定时返回 None。
    """
    out = _git_status_porcelain(repo)
    if out is None:
        return None
    files = set()
    for line in out.splitlines():
        line = line.rstrip('\n')
        if not line.strip():
            continue
        # porcelain 前两字符为 XY 状态，其后为空格 + 路径（重命名形如 "a -> b"）
        body = line[3:].strip() if len(line) > 3 else line.strip()
        if ' -> ' in body:
            body = body.split(' -> ', 1)[1]
        if body:
            files.add(body)
    return files


# 符合项目规范、可被安全自动清理的临时文件名特征（避免误删真实改动）。
_TEMP_NAME_HINTS = ('_commit_msg', '.tmp', '.bak', '.orig', '~',
                    'tmp_', 'scratch', 'test_tmp')


def _looks_temporary(path: str) -> bool:
    base = os.path.basename(path).lower()
    if base in ('_commit_msg.txt',):
        return True
    for h in _TEMP_NAME_HINTS:
        if h in base:
            return True
    return False


def _verify_and_report_clean(repo: str, baseline_dirty, head_before, reply: str):
    """做事后结构核查：保证本次 AI 运行没有在仓库留下未提交的脏文件。

    返回 (reply, clean_bool)。逻辑：
    - 非 git 仓库 / 无法判定：跳过重构，返回 clean=True；
    - 计算「新增脏文件」= 当前脏文件 − 运行前基线脏文件（仅这些可归因于本次运行）；
    - 对符合项目规范的可丢弃临时文件（_commit_msg.txt / *.tmp 等）先自动清理；
    - 若清理后仍有残留脏文件：在回复中追加告警，列出文件，提示人工提交或清理；
    - 若运行前就存在、运行后依旧脏的基线文件：仅作温和提醒（不归因于本次）。

    该检查与「模型是否自觉提交」无关，由进程客观比对仓库状态，从而在结构上
    保证 git 仓库干净——即使模型漏提交/漏清理临时脚本，也会被兜底发现并处置。
    """
    if baseline_dirty is None:
        return reply, True
    current = _git_dirty_files(repo)
    if current is None:
        return reply, True

    new_dirty = current - baseline_dirty           # 本次运行新增的可疑脏文件
    leftover_baseline = baseline_dirty & current     # 运行前已脏、运行后依旧脏

    if not new_dirty and not leftover_baseline:
        return reply, True

    # 先尝试清理符合项目规范的临时文件
    removed = []
    for f in list(new_dirty):
        full = os.path.join(repo, f)
        if _looks_temporary(f) and os.path.isfile(full):
            try:
                os.remove(full)
                new_dirty.discard(f)
                removed.append(f)
            except Exception:
                pass

    parts = []
    if not new_dirty:
        msg = '⚠️ 检测到本次任务产生了未提交文件，已按项目规范自动清理临时文件：' \
              + '、'.join(removed) + '。仓库现已恢复干净。'
        parts.append(msg)
    else:
        msg = ('⚠️ git 仓库未保持干净：本次任务遗留了未提交的改动/文件，'
               '请人工确认并提交或清理（不要遗留临时脚本）：')
        msg += '\n' + '\n'.join('- ' + f for f in sorted(new_dirty))
        if removed:
            msg += '\n（已自动清理临时文件：' + '、'.join(removed) + '）'
        parts.append(msg)

    if leftover_baseline:
        parts.append('（提示：任务开始前仓库即存在未提交改动，仍遗留：'
                     + '、'.join(sorted(leftover_baseline))
                     + '；这些不归因于本次任务，建议另行处理）')

    reply = (reply or '') + '\n\n' + '\n'.join(parts)
    return reply, (len(new_dirty) == 0)


# 用户意图四类：建议 / 缺陷 / 继续 / 闲聊（用于分阶段进度展示与模型对齐参考）。
_INTENT_LABELS = {
    'suggestion': '建议',
    'defect': '缺陷',
    'continue': '继续',
    'chat': '闲聊',
}


def _classify_intent(prompt: str) -> str:
    """确定性判断用户诉求的意图类别，供阶段进度展示与模型对齐参考。

    返回 'suggestion'（建议）/ 'defect'（缺陷）/ 'continue'（继续）/ 'chat'（闲聊）。
    优先级：继续 > 缺陷 > 建议 > 闲聊。纯问候 / 无实质任务视为闲聊。
    该判断由宿主进程基于关键词客观完成（不依赖模型输出），从而在结构上保证
    「每条消息先判断意图」这一环节必然发生并被展示，不靠提示词兜底。
    """
    p = (prompt or '').lower()
    continue_kw = ('继续', '接着', '然后呢', '还有呢', '再处理', '上一个', '刚才那个',
                   '之前那个', '刚刚说的', '上面那个', '进一步', '再帮我')
    defect_kw = ('bug', '错误', '异常', '故障', '失败', '崩溃', '不显示', '空白',
                 '不动', '修复', '排查', '报错', '卡死', '不对', '没反应', '没生效',
                 '不行', '问题', '出现', '闪退')
    suggestion_kw = ('建议', '功能', '特性', '优化', '新增', '支持', '需求', '实现',
                    '增强', '希望', '能不能', '能否', '最好', '应该')
    chat_kw = ('你好', '您好', 'hi', 'hello', '在吗', '谢谢', '感谢', '哈哈', '哦', '嗯', '好的')
    if any(k in p for k in continue_kw):
        return 'continue'
    if any(k in p for k in defect_kw):
        return 'defect'
    if any(k in p for k in suggestion_kw):
        return 'suggestion'
    if not p or len(p) < 6 or any(k in p for k in chat_kw):
        return 'chat'
    return 'chat'


def _classify_work_category(prompt: str) -> str:
    """将用户诉求映射为反馈中心类型（bug / suggestion / other），供跟踪单归类。

    直接复用意图判定结果：缺陷 -> bug，建议 -> suggestion，其余 -> other。
    """
    intent = _classify_intent(prompt)
    return {'defect': 'bug', 'suggestion': 'suggestion'}.get(intent, 'other')


# 标题提炼时去除的开头命令/填充词（这些不属于「问题概括」本身）。
_TITLE_LEAD_FILLERS = (
    '你来解决这个问题', '你来解决', '你来处理', '排查一下', '排查', '修复一下', '修复',
    '优化一下', '优化', '改一下', '改下', '看一下', '看看', '继续', '帮我', '请',
    '现在', '稍后', '我怀疑', '你说', '刚才', '当前', '最近', '这个', '那个',
)

# 执行阶段未产出正文时 reply 的占位提示，不应作为「解决说明」留言落库。
_EXEC_PLACEHOLDER = '（任务已执行完成，无文本输出）'

# 用户消息中对既有反馈单的引用形如 #202608130018（12 位：yyyymmdd+4 位流水）。
_REF_ISSUE_RE = re.compile(r'#(\d{12})')


def _extract_ref_issue(prompt):
    """从用户消息里提取被引用的既有反馈单号（形如 #202608130018）；无则返回 None。"""
    if not prompt:
        return None
    m = _REF_ISSUE_RE.search(prompt)
    return m.group(1) if m else None


def _add_feedback_comment(issue_id, content):
    """向既有反馈单以「自动助手」身份追加一条留言（分析/解决说明），返回是否成功。

    仅用于「用户引用了既有反馈单」时，把本次 AI 的分析与解决回复进该单（满足
    「在反馈单中回复」的诉求）。主服务不可达则该条留言暂未落盘，返回 False 由调用方提示重试。
    """
    content = (content or '').strip()
    if not content:
        return False
    try:
        from platform_client import add_feedback_comment
        return bool(add_feedback_comment(str(issue_id), content))
    except Exception:
        return False


def _make_ticket_title(prompt: str) -> str:
    """从用户诉求中结构化提炼一句话「概括」作为反馈单标题（不依赖模型输出）。

    反馈单标题应是对问题的概括，而非原始诉求的整段照抄。本函数取首行首句、
    剥离开头命令/填充词、截断到合适长度，得到一个干净的问题概括标题。
    """
    if not prompt or not prompt.strip():
        return 'AI 处理任务'
    # 纯填充词（如「继续」「帮我」）无法提炼出问题概括，回退到通用标题，
    # 避免把一句命令词当成反馈单标题（例如「继续」被提炼成标题「继续」）。
    p0 = (prompt or '').strip()
    if p0 in _TITLE_LEAD_FILLERS:
        return 'AI 处理任务'
    # 取首行，并按首个断句符截断到更紧凑的概括
    line = prompt.strip().split('\n', 1)[0].strip()
    for sep in ('。', '；', ';', '？', '?', '！', '!'):
        if sep in line:
            line = line.split(sep, 1)[0].strip()
            break
    # 去掉开头的命令/填充词，保留问题本质
    for f in _TITLE_LEAD_FILLERS:
        if line.startswith(f):
            line = line[len(f):].strip()
            break
    # 剥离开头残留的冒号/标点（如「你来解决这个问题：...」）
    line = line.lstrip('：: ').strip()
    line = line.strip(' ，,。.：:')
    if not line:
        line = prompt.strip().split('\n', 1)[0].strip()
    if len(line) > 40:
        line = line[:40] + '…'
    return line or 'AI 处理任务'


# ---------------------------------------------------------------------------
# 去重续写：处理缺陷/建议前，先查反馈中心是否已有覆盖同一问题的未关闭单。
# 命中则继续跟踪旧单（把本次分析/解决以「自动助手」身份续写进旧单），不重复建单——
# 解决「问题没修好又来反馈、用户懒得找旧单」场景下重复建单的问题。
# ---------------------------------------------------------------------------
def _normalize_issue_text(s: str) -> str:
    """归一化文本：去空白与标点（含中文标点），保留中英文/数字，便于相似度比对。"""
    if not s:
        return ''
    s = s.lower()
    return re.sub(r'[\s\W_]+', '', s)


def _longest_common_substring_len(a: str, b: str) -> int:
    """a、b 最长公共连续子串长度（滚动数组 DP，文本较短足够快）。"""
    if not a or not b:
        return 0
    n, m = len(a), len(b)
    prev = [0] * (m + 1)
    best = 0
    for i in range(1, n + 1):
        cur = [0] * (m + 1)
        ai = a[i - 1]
        for j in range(1, m + 1):
            if ai == b[j - 1]:
                cur[j] = prev[j - 1] + 1
                if cur[j] > best:
                    best = cur[j]
        prev = cur
    return best


def _trigram_jaccard(a: str, b: str) -> float:
    """字符三元组 Jaccard 相似度，用于捕捉「复述/换表述」的同类反馈。"""
    def grams(s):
        s = _normalize_issue_text(s)
        if len(s) < 3:
            return {s} if s else set()
        return {s[i:i + 3] for i in range(len(s) - 2)}
    ga, gb = grams(a), grams(b)
    if not ga or not gb:
        return 0.0
    union = len(ga | gb)
    return (len(ga & gb) / union) if union else 0.0


def _find_existing_ticket(prompt: str, ftype: str):
    """在反馈中心查找是否已有覆盖同一问题的「未关闭」反馈单。

    返回可续写的单号（str）或 None。命中即意味着本次无需重复建单，应继续跟踪旧单。
    判定偏高精度（避免误合并不同问题）：新诉求归一化后，要么过半内容命中某旧单
    （最长公共子串占比 >= 0.5），要么存在较长公共片段且整体三元组相似度较高。
    仅匹配 open/in_progress/pending_verification（不含已关闭/已验证/已驳回），
    避免把已解决的问题误重新打开。
    """
    prompt = (prompt or '').strip()
    if len(prompt) < 10:
        return None
    if ftype not in ('bug', 'suggestion', 'other'):
        return None
    try:
        from platform_client import search_feedback_issues
        candidates = search_feedback_issues(prompt, ftype) or []
    except Exception:
        return None
    if not candidates:
        return None
    norm_p = _normalize_issue_text(prompt)
    if not norm_p:
        return None
    best_id = None
    best_score = 0.0
    for it in candidates:
        hay = (it.get('title') or '') + ' ' + (it.get('content') or '')
        norm_hay = _normalize_issue_text(hay)
        if not norm_hay:
            continue
        lcs = _longest_common_substring_len(norm_p, norm_hay)
        contain = lcs / max(1, len(norm_p))
        # 包含度为主判据；触发较长公共片段时，辅以三元组相似度捕捉换表述复述
        if contain >= 0.5:
            score = contain
        elif lcs >= 10 and _trigram_jaccard(prompt, hay) >= 0.35:
            score = _trigram_jaccard(prompt, hay)
        else:
            score = 0.0
        if score > best_score:
            best_score = score
            best_id = it.get('id')
    return best_id if best_score > 0 else None


def _maybe_create_tracking_ticket(task_id, prompt, owner_id, reply, filed_id, head_before,
                                   git_clean=True, intent=None, analysis=None, resolution=None,
                                   title=None):
    """结构性保证：当 AI 本回合实际改动代码（产生新提交，HEAD 变化）且未通过
    feedback-request 建单时，于反馈中心创建一张「跟踪单」（状态 pending_verification），
    记录处理动作与提交哈希，供管理员验证后手动关闭。返回 (reply, track_id, s_ticket)。

    该逻辑不依赖模型是否「主动」输出反馈块，而是由仓库状态客观判定，从而在结构上
    保证「AI 处理新特性或问题」必有反馈中心单跟踪——即使模型漏提反馈块也不会漏单。
    git_clean 标记本回合结束后仓库是否干净（见 _verify_and_report_clean），一并写入
    跟踪单 extra，便于管理员验证时核对。

    反馈单留言（均「自动助手」身份）：把一次处理拆成多条留言，而非一条大段——
    - 首条留言 = 分析定位（问题根因，来自阶段3的 analysis）；
    - 后续留言 = 解决说明（AI 实际做了什么/修复内容，来自阶段4的 resolution）。
    这样反馈中心里能看到 AI 的「分析 → 解决」完整处理记录，而非只有一句话概括。
    """
    repo = _project_root()
    head_after = _git_rev_head(repo)
    # 仅在确有新提交（HEAD 变化）时视为「处理了一个新特性/问题」
    if not head_after or head_after == head_before:
        return reply, None, None
    # 本回合已通过反馈块建单，则不再重复建跟踪单
    if filed_id:
        return reply, None, '已在反馈中心提交反馈单（本回合用户反馈路径）'
    # 标题=对问题的概括（结构性提炼，不照抄原始诉求；可经 title 参数覆盖，
    # 例如「继续」但无历史单时退化为上一条问题的概括，避免把「继续」当标题）；
    # 内容=问题描述（用户诉求原文）；
    # 留言=分析根因（首条）+ 解决说明（后续），均为「自动助手」身份。
    title = title or _make_ticket_title(prompt)
    content = (prompt or '').strip() or '(无问题描述)'
    # 分析留言（根因），去重自引用提示。
    analysis_txt = (analysis or '').strip()
    # 解决说明留言，剔除末尾自动追加的「已创建跟踪单 / 处理已完成」提示，避免自引用。
    resolution_txt = (resolution or '').strip()
    # 执行阶段若未产出正文，reply 会被置为占位提示，不应作为「解决说明」落库成噪声留言。
    if resolution_txt == _EXEC_PLACEHOLDER:
        resolution_txt = ''
    for marker in ('\n\n📋 已创建处理跟踪单', '（⚠️ 处理已完成', '⚠️ 处理已完成'):
        if marker in resolution_txt:
            resolution_txt = resolution_txt.split(marker)[0].strip()
    if len(resolution_txt) > 4000:
        resolution_txt = resolution_txt[:4000] + '…（已截断）'
    # 缺陷/建议处理：先查反馈中心是否已有覆盖同一问题的「未关闭」单。
    # 命中则把本次分析/解决以「自动助手」身份续写进旧单，继续跟踪而非重复建单——
    # 解决「问题没修好又来反馈、用户懒得找旧单」场景下重复建单的问题。
    ftype = {'defect': 'bug', 'suggestion': 'suggestion'}.get(intent, _classify_work_category(prompt))
    existing_id = _find_existing_ticket(prompt, ftype)
    if existing_id:
        if analysis_txt:
            _add_feedback_comment(existing_id, analysis_txt)
        if resolution_txt:
            _add_feedback_comment(existing_id, resolution_txt)
        track_id = existing_id
        s_ticket = ('📋 已在反馈单 #%s 中继续跟踪：同一问题此前已有未关闭单，'
                    '本次分析/解决已以「自动助手」身份续写进该单（未重复建单）') % existing_id
        reply = (reply or '') + '\n\n' + s_ticket
        return reply, track_id, s_ticket
    # 首条留言优先取分析根因；若无分析则退而取解决说明作为首条。
    first_comment = analysis_txt or None
    extra_comments = []
    if resolution_txt:
        extra_comments.append(resolution_txt)
    if not first_comment and extra_comments:
        first_comment = extra_comments.pop(0)
    extra = {'git_commit': head_after, 'task_id': task_id,
             'owner_id': owner_id, 'track': True, 'git_clean': git_clean}
    track_id = _file_feedback(
        ftype, title, content,
        extra=extra, status='pending_verification',
        comment=first_comment, comments=extra_comments or None)
    if track_id:
        s_ticket = '📋 已创建处理跟踪单：#%s（状态：待验证，可在反馈中心查看）' % track_id
    else:
        # file_feedback 已对「主服务暂不可达」做本地 spool 兜底，待其恢复后自动补建；
        # 此处仅做透明提示，避免用户误以为处理丢失。
        s_ticket = '（⚠️ 处理已完成，已尝试在反馈中心建立跟踪单；若主服务暂不可达将自动重试建单）'
    # 把建单提示拼回 reply，确保用户最终回复里能看到跟踪单号（与阶段结论一致）。
    reply = (reply or '') + '\n\n' + s_ticket
    return reply, track_id, s_ticket


# 对话系统约束：本助手具备真实执行能力，要求直接动手而非只描述。
_SYSTEM_PROMPT = (
    '你是一个嵌入在媒体库管理后台里的 AI 助手，拥有读写文件、运行命令的真实能力。\n'
    '当用户布置具体任务（如修改代码、创建/删除文件、执行命令等）时，请直接动手完成，'
    '不要只罗列步骤或描述做法；完成后用简体中文简要说明你做了什么。\n'
    '完成后必须保持 git 仓库干净：改动要提交（提交消息用文件方式写 UTF-8 中文，'
    '保持原有提交身份，不要改成「自动助手」），临时脚本/截图/中间产物必须删除，'
    '不得遗留未提交文件。\n'
    '若只是闲聊或提问，则正常简洁回答即可。\n'
    '\n'
    '【提交反馈】当用户的消息是在向本产品提交一条新的反馈（例如报告一个 bug、或提出一个'
    '建议 / 功能诉求）时：\n'
    '- 不要把它当作「需要你去修复或实现的任务」，也不要只描述做法；\n'
    '- 把反馈整理为简洁的 title 与 content，并在你回复的【最末尾】追加一个如下格式的围栏代码块：\n'
    '  ```feedback-request\n'
    '  {"type":"bug 或 suggestion","title":"一句话标题","content":"反馈的详细描述"}\n'
    '  ```\n'
    '- 在你的正文里用占位符 #(待分配) 表示反馈单号，并告知用户已提交、可在反馈中心查看；'
    '例如：「已为你提交反馈单 #(待分配)（类型：bug），我们会跟进处理。」\n'
    '- 若用户只是普通提问、闲聊，或让你执行某项任务，则按正常规则处理，不要输出 feedback-request 块。\n'
    '- 若用户引用了某个已有反馈单（形如 #202608120001），正常与其讨论该问题即可，该单号可被点击跳转。\n'
    '\n'
    '【续接既有反馈单】处理缺陷或建议时，若反馈中心已存在覆盖同一问题的未关闭单，'
    '系统会自动继续跟踪该旧单（把本次分析与解决以「自动助手」身份续写进旧单），而不重复建单；'
    '你无需手动查找旧单号，只需在回复中说明已续接到哪张单即可。\n'
    '\n'
    '【引用资源库资源】当你的回复涉及本媒体库里的具体资源（视频 / 图集 / 帖子 / 文本），'
    '请用 Markdown 链接形式引用，用户点击即可跳转到该资源详情页：\n'
    '  [资源显示名](dbox://resource/<类型>/<标识>)\n'
    '<类型> 为 video / gallery / post / text 之一；<标识> 优先用资源的真实标识——'
    '视频/图集用其 hash（64 位十六进制字符串，videos/galleries 表的 hash 列），帖子/文本用整数 id（posts/texts 表的 id 列）。'
    '若你只知道资源标题，也可把 <标识> 写成标题关键字，系统会按标题模糊匹配解析。\n'
    '要在库里查找资源的真实标识，可用 Bash 工具直接查询媒体库数据库（Python 内置 sqlite3，无需额外依赖）。\n'
    '【重要】你「能看到的资源列表必须与资源管理器一致」：只能引用归属「已激活资源库」的资源，'
    '不得暴露已停用（is_active=0）资源库里的内容。因此查询必须 JOIN 已激活库并排除隐藏/已删除资源，例如：\n'
    '  python -c "import sqlite3,os; p=os.path.join(os.environ.get(\'DBOX_DATA_DIR\',\'data\'),\'databases\',\'dbox.db\'); c=sqlite3.connect(p); [print(r) for r in c.execute(\"SELECT v.hash, v.title FROM videos v JOIN resource_index ri ON v.resource_index_id=ri.id JOIN resource_libraries rl ON ri.library_id=rl.id WHERE rl.is_active=1 AND ri.hidden=0 AND v.in_trash=0 AND v.title LIKE \'%关键字%\' LIMIT 5\")]"\n'
    '（图集表 galleries 同构，把 v 换成 g、v.resource_index_id 换成 g.resource_index_id 即可；'
    '帖子表 posts 用 p.library_id 关联 resource_libraries 且 p.in_trash=0，文本表 texts 经 resource_index 关联；'
    '所有查询都务必带 rl.is_active=1 这一条件。）\n'
    '仅在确实引用到某个具体资源时才使用此链接；闲聊或泛泛而谈时不要编造引用；'
    '若某资源归属的库已停用，不要引用它。'
)


# 解析 AI 回复中的 feedback-request 围栏块（AI 用其对反馈中心提单，后端执行建单并回填单号）。
# 容忍围栏内可选的空白/换行差异，降低模型格式偏差导致漏单的概率。
_FB_RE = re.compile(r'```\s*feedback-request\s*\n?(.*?)```', re.DOTALL)


class AIChatManager:
    # 任务状态
    STATUS_PENDING = 'pending'
    STATUS_RUNNING = 'running'
    STATUS_COMPLETED = 'completed'
    STATUS_FAILED = 'failed'
    STATUS_CANCELLED = 'cancelled'

    def __init__(self):
        self._db = None
        self._db_path = None
        self._lock = threading.RLock()
        self._queue = queue.Queue()          # FIFO：仅存待执行的 task_id
        self._worker = None
        self._procs = {}                      # task_id -> subprocess.Popen（用于取消）
        self._cancel = {}                    # task_id -> True（取消标记）
        self._head_before = {}               # task_id -> 任务开始前的仓库 HEAD（回滚基线）
        self._skip = set()                   # 已在排队但被用户删除的 task_id
        self._subscribers = {}               # task_id -> [queue.Queue, ...]（SSE 订阅者）
        self._buffers = {}                    # task_id -> [token, ...]（运行期已产出的 token，供重连续接）
        self._phase_log = {}                  # task_id -> [phase dict, ...]（运行期各阶段的状态日志，
                                             #   phase dict = {index,label,kind,state,conclusion,body}，
                                             #   作为 SSE 重连与 2s 轮询重建「每阶段一个气泡」时间线的唯一可信源，
                                             #   根治「早期阶段在 SSE 连上前已发射而丢失」的问题）
        self._cur_phase = {}                  # task_id -> 当前阶段 index（token 流式填充归属该阶段气泡）
        self._track_ids = {}                   # task_id -> 关联的反馈跟踪单号（跨任务回溯续写用）
        # 工作流 ask 步骤的挂起/恢复：worker 在 ask 步阻塞等待，answer_task 写入答案并唤醒。
        self._pending_answers = {}             # task_id -> {step_id: choice}
        self._answer_events = {}               # task_id -> threading.Event
        self._initialized = False
        # 统一任务表镜像（轻量，仅状态展示）
        self._ut = None
        # 纯插件宿主注入的依赖（由 server.create_blueprint 经 host 注入）
        self._vault = None
        self._tasks = None

    # ---------- 初始化 ----------
    def init(self, data_dir, vault=None, tasks=None):
        """初始化管理器。vault/tasks 为纯插件模式下宿主注入的依赖代理；
        若未注入则回退到直接 import 框架内部模块（向后兼容）。"""
        self._vault = vault or self._vault
        self._tasks = tasks or self._tasks
        if self._initialized and self._db_path:
            return
        os.makedirs(data_dir, exist_ok=True)
        self._db_path = os.path.join(data_dir, 'ai_assistant.db')
        self._db = sqlite3.connect(self._db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._init_db()
        # 启动 worker（幂等）
        if self._worker is None or not self._worker.is_alive():
            self._worker = threading.Thread(target=self._worker_loop, daemon=True)
            self._worker.start()
        # 启动时把残留的 running/pending 任务复位，避免死任务卡住队列
        self._recover_stale_tasks()
        # 启动反馈建单 spool 的兜底重放：主服务若此前离线，积压的「AI 处理跟踪单 /
        # 用户反馈单」在此立即补建一次，并由周期任务持续重试，直到主服务恢复。
        self._start_feedback_spool_flusher()
        # 挂接统一任务表：优先使用宿主注入的 tasks 代理（纯插件模式）
        if self._tasks is not None:
            self._ut = 'host'
        else:
            try:
                from shared.unified_tasks import init_task_manager as _init_tm
                _init_tm(data_dir)
                self._ut = True
            except Exception:
                self._ut = None
        self._initialized = True

    def _init_db(self):
        with self._lock:
            self._db.execute('''CREATE TABLE IF NOT EXISTS ai_assistant_tasks (
                task_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                prompt TEXT NOT NULL,
                reply TEXT,
                error TEXT,
                owner_id INTEGER,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )''')
            # 分阶段气泡：将每个阶段（标签 + 结论/正文）以 JSON 存储，
            # 使历史回看也能逐阶段还原为独立气泡，而非仅剩模型的一段最终文本。
            try:
                self._db.execute('ALTER TABLE ai_assistant_tasks ADD COLUMN phases TEXT')
            except Exception:
                pass
            # 关联反馈跟踪单号：供「继续」意图回溯上一条问题对应的反馈单并续写留言，
            # 而非新建一张标题为「继续」的脏单。
            try:
                self._db.execute('ALTER TABLE ai_assistant_tasks ADD COLUMN track_id TEXT')
            except Exception:
                pass
            # 工作流选择：存储 workflow_id / manual 等元数据（JSON 文本）。
            try:
                self._db.execute('ALTER TABLE ai_assistant_tasks ADD COLUMN extra TEXT')
            except Exception:
                pass
            # 回滚基线：记录任务开始处理前的仓库 HEAD，供「停止后撤销本次改动」精准回退。
            try:
                self._db.execute('ALTER TABLE ai_assistant_tasks ADD COLUMN head_before TEXT')
            except Exception:
                pass
            self._db.execute('CREATE INDEX IF NOT EXISTS idx_ai_assistant_tasks_status ON ai_assistant_tasks(status)')
            self._db.execute('CREATE INDEX IF NOT EXISTS idx_ai_assistant_tasks_created ON ai_assistant_tasks(created_at)')
            self._db.commit()

    def _start_feedback_spool_flusher(self):
        """启动反馈建单 spool 的周期重放：确保主服务离线期间积压的跟踪单/反馈单
        在其恢复后被自动补建，从而在结构上保证「AI 处理必有反馈中心单跟踪」不丢单。"""
        try:
            from platform_client import flush_feedback_spool, _internal_secret
        except Exception:
            return

        # 启动即诊断内部密钥是否可发现：若读不到密钥，所有 /internal/* 调用都会 401，
        # 建单会全部静默失败。明确告警，避免再次出现「机制完全不生效却无提示」。
        if not _internal_secret():
            _logger.error(
                '反馈建单诊断：未找到主服务内部密钥（.dbox_internal_key）。'
                '拓展宿主与主服务数据目录不一致会导致 /internal/* 调用被 401 拒绝，'
                'AI 处理将无法正常在反馈中心建单。请检查 DBOX_DATA_DIR 或 '
                '%s\\Dbox\\data 下的密钥文件。' % (os.environ.get('ProgramData', 'C:\\ProgramData'))
            )

        def _loop():
            while True:
                try:
                    created = flush_feedback_spool()
                    if created:
                        _logger.info('反馈 spool 重放成功，补建单号：%s', ','.join(created))
                except Exception:
                    pass
                time.sleep(60)

        # 启动时先补建一次，再起守护线程周期重试
        try:
            created = flush_feedback_spool()
            if created:
                _logger.info('反馈 spool 启动补建成功，单号：%s', ','.join(created))
        except Exception:
            pass
        t = threading.Thread(target=_loop, daemon=True)
        t.start()

    def _recover_stale_tasks(self):
        """服务重启后，把未完成（pending/running）的任务复位为 cancelled/failed，防止 worker 卡死。"""
        now = time.time()
        with self._lock:
            rows = self._db.execute(
                'SELECT task_id, status FROM ai_assistant_tasks WHERE status IN (?, ?)',
                (self.STATUS_PENDING, self.STATUS_RUNNING)).fetchall()
            for r in rows:
            # pending 直接取消；running（进程已不在）标记为失败
                new = self.STATUS_CANCELLED if r['status'] == self.STATUS_PENDING else self.STATUS_FAILED
                self._db.execute(
                    'UPDATE ai_assistant_tasks SET status=?, error=?, updated_at=? WHERE task_id=?',
                    (new, '服务重启，任务已重置' if new == self.STATUS_CANCELLED else '服务重启，任务中断',
                     now, r['task_id']))
            self._db.commit()

    # ---------- 内部 DB 辅助 ----------
    def _now(self):
        return time.time()

    def _row_to_dict(self, row):
        if not row:
            return None
        d = dict(row)
        # extra 列以 JSON 文本存储，取出时反序列化为 dict，避免上层当作字符串调用 .get()
        if 'extra' in d and isinstance(d.get('extra'), str):
            try:
                d['extra'] = json.loads(d['extra']) if d['extra'] else {}
            except Exception:
                d['extra'] = {}
        return d

    def get_task(self, task_id):
        with self._lock:
            row = self._db.execute('SELECT * FROM ai_assistant_tasks WHERE task_id=?', (task_id,)).fetchone()
        return self._row_to_dict(row) if row else None

    def _insert_task(self, task_id, prompt, owner_id, status, extra=None):
        now = self._now()
        extra_json = json.dumps(extra, ensure_ascii=False) if extra else None
        with self._lock:
            self._db.execute(
                'INSERT INTO ai_assistant_tasks (task_id, status, prompt, reply, error, owner_id, created_at, updated_at, track_id, extra) '
                'VALUES (?,?,?,NULL,NULL,?,?,?,NULL,?)',
                (task_id, status, prompt, owner_id, now, now, extra_json))
            self._db.commit()
        self._sync_to_unified(task_id, prompt, owner_id, status, now, now)

    def _set_head_before(self, task_id, head_before):
        """把任务开始处理前的仓库 HEAD 持久化，作为停止后「撤销本次改动」的回滚基线。"""
        if not head_before:
            return
        with self._lock:
            self._head_before[task_id] = head_before
            self._db.execute('UPDATE ai_assistant_tasks SET head_before=? WHERE task_id=?',
                             (head_before, task_id))
            self._db.commit()

    def _set_track_id(self, task_id, track_id):
        """把本次任务关联的反馈跟踪单号持久化，供后续「继续」任务回溯续写。"""
        if not track_id:
            return
        with self._lock:
            self._track_ids[task_id] = track_id
            self._db.execute('UPDATE ai_assistant_tasks SET track_id=? WHERE task_id=?',
                             (str(track_id), task_id))
            self._db.commit()

    def _find_prev_track_id(self, owner_id):
        """查找最近一条「已建跟踪单」的反馈单号，供「继续」意图续写其分析与解决。

        优先在同 owner 下查找；若 owner 未知（None）或该 owner 无历史单，
        则回退到全局最近一条，确保「继续」总能回溯到上一条问题对应的反馈单。
        """
        with self._lock:
            if owner_id is not None:
                row = self._db.execute(
                    "SELECT track_id FROM ai_assistant_tasks WHERE track_id IS NOT NULL AND track_id != '' "
                    "AND owner_id=? AND status=? ORDER BY updated_at DESC LIMIT 1",
                    (owner_id, self.STATUS_COMPLETED)).fetchone()
                if row:
                    return row['track_id']
            row = self._db.execute(
                "SELECT track_id FROM ai_assistant_tasks WHERE track_id IS NOT NULL AND track_id != '' "
                "AND status=? ORDER BY updated_at DESC LIMIT 1",
                (self.STATUS_COMPLETED,)).fetchone()
        return row['track_id'] if row else None

    def _set_status(self, task_id, status, reply=None, error=None, phases=None):
        now = self._now()
        with self._lock:
            if reply is not None:
                if phases is not None:
                    self._db.execute('UPDATE ai_assistant_tasks SET status=?, reply=?, phases=?, updated_at=? WHERE task_id=?',
                                     (status, reply, phases, now, task_id))
                else:
                    self._db.execute('UPDATE ai_assistant_tasks SET status=?, reply=?, updated_at=? WHERE task_id=?',
                                     (status, reply, now, task_id))
            elif error is not None:
                self._db.execute('UPDATE ai_assistant_tasks SET status=?, error=?, updated_at=? WHERE task_id=?',
                                 (status, error, now, task_id))
            else:
                self._db.execute('UPDATE ai_assistant_tasks SET status=?, updated_at=? WHERE task_id=?',
                                 (status, now, task_id))
            self._db.commit()
        self._sync_to_unified_by_id(task_id, status)

    # ---------- 统一任务表镜像（轻量，仅展示状态/标题） ----------
    def _sync_to_unified(self, task_id, prompt, owner_id, status, created_at, updated_at):
        if not self._ut:
            return
        title = (prompt or '').strip().replace('\n', ' ')
        if len(title) > 60:
            title = title[:60] + '…'
        title = title or 'AI 对话'
        try:
            if self._tasks is not None:
                self._tasks.create(title=title, owner_id=owner_id or 0, status=status)
            else:
                from shared.unified_tasks import create_task
                create_task('ai:' + task_id, 'ai_assistant', title, owner_id=owner_id,
                            status=status, created_at=created_at, updated_at=updated_at)
        except Exception:
            pass

    def _sync_to_unified_by_id(self, task_id, status):
        if not self._ut:
            return
        try:
            if self._tasks is not None:
                # 插件代理以 kind 维度注册，这里仅更新状态映射（轻量展示）
                return
            from shared.unified_tasks import update_task, get_task as ut_get
            ut_id = 'ai:' + task_id
            if ut_get(ut_id) is None:
                t = self.get_task(task_id)
                if t:
                    self._sync_to_unified(task_id, t['prompt'], t['owner_id'], status, t['created_at'], t['updated_at'])
                return
            update_task(ut_id, status=status)
        except Exception:
            pass

    def _remove_from_unified(self, task_id):
        if not self._ut:
            return
        try:
            if self._tasks is not None:
                return
            from shared.unified_tasks import delete_task
            delete_task('ai:' + task_id, is_admin=True)
        except Exception:
            pass

    # ---------- 入队 ----------
    def enqueue(self, message, owner_id=None, workflow_id=None, manual=False, plan_mode=False, model=None):
        """把一条用户消息作为任务入队，立即返回 task_id（不阻塞）。

        workflow_id: 用户显式选择的工作流（来自前端按钮）；为 None 时由 worker 实时推断。
        manual:      是否为用户手动设置（手动设置后前端不再要求推断）。
        plan_mode:   计划模式——AI 只产出修改计划文档（md），绝不实际改代码。
                      执行阶段（用户点「执行」）才以普通任务重新提交计划内容。
        model:       可选模型 ID（如 deepseek-v4-flash）。为空则使用 CLI 默认模型。
        """
        msg = (message or '').strip()
        if not msg:
            return None, 'message 必填'
        extra = {}
        if model:
            extra['model'] = model
        if workflow_id:
            extra['workflow_id'] = workflow_id
            extra['manual'] = bool(manual)
        if plan_mode:
            # plan 模式强制使用 plan 工作流，AI 仅生成计划文档
            extra['workflow_id'] = 'plan'
            extra['manual'] = True
            extra['plan_mode'] = True
        task_id = 'ai_' + uuid.uuid4().hex[:16]
        self._insert_task(task_id, msg, owner_id, self.STATUS_PENDING, extra=extra or None)
        self._queue.put(task_id)
        return task_id, None

    def list_workflows(self):
        """返回全部工作流元信息，供前端渲染选择面板。"""
        from workflow_engine import get_engine
        return get_engine().list_meta()

    # ---------- worker ----------
    def _terminate(self, proc):
        """强制结束进程及其子进程树（Windows 用 taskkill /T，类 Unix 用 killpg）。"""
        if not proc:
            return
        pid = proc.pid
        try:
            if os.name == 'nt':
                subprocess.run(['taskkill', '/F', '/T', '/PID', str(pid)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            else:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
        except Exception:
            pass
        try:
            proc.kill()
        except Exception:
            pass

    def _worker_loop(self):
        while True:
            task_id = self._queue.get()
            if task_id is None:
                break
            with self._lock:
                if task_id in self._skip:
                    self._skip.discard(task_id)
                    self._set_status(task_id, self.STATUS_CANCELLED, error='已取消')
                    continue
            try:
                self._process(task_id)
            except Exception as e:
                import traceback as _tb
                _detail = _tb.format_exc()
                logging.getLogger('extensions_host').error('AI 助手 _process 异常:\n%s', _detail)
                self._set_status(task_id, self.STATUS_FAILED, error='处理异常: ' + str(e))
                self._emit(task_id, 'error', '处理异常: ' + str(e))

    def _context_turns(self, exclude_id=None, limit=20):
        """构建多轮上下文：取已完成且含回复的任务，按时间正序，最近 limit 轮。"""
        with self._lock:
            rows = self._db.execute(
                "SELECT prompt, reply FROM ai_assistant_tasks WHERE status=? AND reply IS NOT NULL "
                "AND reply != '' ORDER BY created_at ASC",
                (self.STATUS_COMPLETED,)).fetchall()
        turns = [(r['prompt'], r['reply']) for r in rows if r['prompt']]
        if exclude_id:
            # exclude 仅影响排序语义，这里简单保留全部（exclude 多属 pending/running，本就不会进上下文）
            pass
        if limit and len(turns) > limit:
            turns = turns[-limit:]
        return turns

    def _build_prompt(self, message, intent=None, phase=None, analysis=None, prev_issue=None,
                      workflow_prompt=None):
        parts = [_SYSTEM_PROMPT]
        # 工作流编译提示（来自配置）：定义本任务的流程指导 / git 检查触发 / 建单等，
        # 优先于旧的意图分支，承载真正的「分阶段/按流程执行」语义。
        if workflow_prompt:
            parts.append(workflow_prompt.strip())
        turns = self._context_turns()
        if turns:
            parts.append('以下是之前的对话记录，供你理解上下文：')
            for up, ar in turns:
                parts.append('用户：' + up)
                parts.append('助手：' + ar)
            parts.append('')
        parts.append('用户问题：' + message)
        # 注入宿主进程判定出的意图，帮助模型与「分阶段判断」的结论对齐（仅供参考）。
        if intent:
            label = _INTENT_LABELS.get(intent, intent)
            parts.append('（系统初步判定本条用户意图为：%s，供你参考）' % label)
        # 「继续」意图：显式告知模型正在延续哪张反馈单，使之基于上一条问题推进，
        # 而非当成一条全新任务（结构层面保证「继续」= 续写既有问题，而非另建脏单）。
        if intent == 'continue' and prev_issue:
            parts.append('（本条为「继续」意图：你正在延续反馈单 #%s 的处理，'
                         '请基于该单已有的分析与上一轮对话上下文继续推进，'
                         '不要把它当成一条全新的任务。）' % prev_issue)
        # 分阶段控制：宿主脚本驱动处理流程，每个阶段只让 AI 产出该阶段内容（即「给出分支」）。
        # 这样一条命令会被拆成「分析定位 -> 执行处理」等多个可见阶段，而非一次性抛出大段文本。
        if phase == 'analyze':
            parts.append('【本阶段任务：仅做分析与定位，不要修改任何文件】'
                         '请基于用户问题定位根因、列出涉及的代码文件、给出修复方案，'
                         '用简体中文条理清晰地说明。不要执行任何写操作。')
        elif phase == 'execute':
            if analysis:
                parts.append('【本阶段任务：执行修改】以下为上一阶段（分析定位）的结论，'
                             '请直接据此执行修改：\n' + analysis)
            parts.append('请执行修改（必要时创建/编辑文件、运行命令验证），完成后提交 git'
                         '（提交消息用文件方式写 UTF-8 中文、保持原有提交身份、清理临时脚本），'
                         '并用简体中文简要说明你做了什么。')
        elif phase == 'chat':
            parts.append('【本阶段任务：生成回复】用户为闲聊或普通提问，直接简洁回答即可。')
        return '\n'.join(parts)

    def _run_cli(self, prompt, task_id, max_turns, model=None):
        """运行一次 buddy CLI（一个处理阶段），实时把产出 token 推送给订阅者。

        返回 (reply, fell_back, err_text, returncode, cancelled)。脚本驱动的阶段状态机
        对每个需要智能的阶段分别调用本方法：分析定位阶段只读取、执行处理阶段才改代码，
        从而把一条用户命令拆成多个可见阶段，AI 在每个阶段只给出该阶段的内容（分支）。

        model: 可选模型 ID（如 deepseek-v4-flash）。为空则使用 CLI 默认模型。

        看门狗与子进程清理逻辑沿用原 _process 的单次调用实现，避免 worker 因 CLI 拉起
        子进程导致 stdout 管道不关闭而卡死。
        """
        buddy = _resolve_buddy_cli()
        if not buddy:
            return None, False, '未找到 CodeBuddy CLI', 1, False
        env = dict(os.environ)
        token = _load_codebuddy_token()
        if token:
            # CodeBuddy CLI 在 -p 模式下使用 CODEBUDDY_API_KEY 鉴权；
            # 同时保留 ANTHROPIC_API_KEY 兜底，避免影响其它潜在用途。
            env[_CODEBUDDY_API_KEY_ENV] = token
            env[_ANTHROPIC_API_KEY_ENV] = token
        # 中国版网络环境（必须，否则连错端点导致认证失败）
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
            '--max-turns', str(max_turns),
            '--add-dir', _project_root(),
            '--input-format', 'text',
            # 流式 JSON 输出 + 增量消息：把思考（thinking_delta）与正文（text_delta）
            # 拆成独立增量事件，从而让前端「边想边显示」思考过程，而非整段完成后才展示。
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
            with self._lock:
                self._procs[task_id] = proc

            def _watchdog():
                self._cancel[task_id] = True
                self._terminate(proc)
            watchdog = threading.Timer(_MAX_TASK_SECONDS, _watchdog)
            watchdog.daemon = True
            watchdog.start()

            try:
                proc.stdin.write(prompt.encode('utf-8'))
                proc.stdin.close()
            except Exception:
                pass

            full = []
            thinking_parts = []
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
                if not line:
                    continue
                piece = line.rstrip('\n')
                # 优先按 stream-json 逐行解析：thinking_delta / text_delta 拆成独立增量，
                # 实时推送。解析失败（告警/旧格式）则整行当作普通正文 token，保持向后兼容。
                try:
                    evt = json.loads(piece)
                except Exception:
                    if piece:
                        full.append(piece)
                        self._append_token(task_id, piece)
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
                    elif t == 'content_block_delta' and isinstance(evt.get('delta'), dict):
                        _d = evt['delta']
                        if _d.get('type') == 'thinking_delta':
                            delta_think = _d.get('thinking') or ''
                        elif _d.get('type') == 'text_delta':
                            delta_text = _d.get('text') or ''
                    elif t == 'assistant' and isinstance(evt.get('message'), dict):
                        for _blk in (evt['message'].get('content') or []):
                            if not isinstance(_blk, dict):
                                continue
                            if _blk.get('type') == 'thinking':
                                delta_think = (_blk.get('thinking') or '')
                            elif _blk.get('type') == 'text':
                                delta_text = (_blk.get('text') or '')
                    elif t == 'result':
                        # 最终结果事件承载干净的最终回复（不含工具调用前的旁白），
                        # 在 text delta 累积为空时作为权威回复兜底。
                        if isinstance(evt.get('result'), str):
                            result_text = evt.get('result')
                if delta_text:
                    full.append(delta_text)
                    self._append_token(task_id, delta_text)
                if delta_think:
                    thinking_parts.append(delta_think)
                    self._append_thinking(task_id, delta_think)
            proc.stdout.close()
            err_text = ''
            try:
                err_text = proc.stderr.read().decode('utf-8', errors='replace') or ''
            except Exception:
                pass
            proc.stderr.close()
            try:
                proc.wait(timeout=30)
            except Exception:
                pass
            finally:
                watchdog.cancel()
                self._terminate(proc)

            cancelled = bool(self._cancel.get(task_id))
            reply, fell_back = _build_reply(full, err_text, proc.returncode)
            # 若 text delta 累积为空（如模型仅产出思考、或增量解析被截断），以最终结果事件兜底。
            if (not reply) and result_text:
                reply = result_text
            return reply, fell_back, err_text, proc.returncode, cancelled
        except Exception as e:
            return None, False, '调用失败: ' + str(e), 1, False

    def _process(self, task_id):
        """配置驱动的阶段状态机：每条用户命令被拆成多个可见「阶段气泡」，由工作流配置
        （extensions/ai_assistant/workflows/*.yaml）驱动。流程：
          1. 工作流选择 —— 用户指定 / 实时推断 / 回落 chat，结论一句话气泡
          2. start 步（来自配置）：shell 检查（如 git 干净度）、ask 提问（如建单/选问题单，挂起等待）
          3. AI 处理 —— 单次 CLI，系统提示由工作流 compile_prompt 注入（分支指导 + 命中的 shell 提示）
          4. end 步（来自配置）：复查 git 等
          5. 建单 —— 改代码类确有必要则建单；resume 选了具体单则续写该单
        """
        from workflow_engine import get_engine
        engine = get_engine()
        task = self.get_task(task_id)
        if not task:
            return
        with self._lock:
            self._buffers[task_id] = []
            self._phase_log[task_id] = []
            self._cur_phase.pop(task_id, None)
            self._set_status(task_id, self.STATUS_RUNNING)
            self._emit(task_id, 'status', 'running')

        # 记录回滚基线：任务开始前（任何 CLI / 建单之前）的仓库 HEAD，
        # 供「停止后撤销本次改动」精准回退到用户发起时的状态。
        try:
            self._set_head_before(task_id, _git_rev_head(_project_root()))
        except Exception:
            pass

        _pi = [0]

        def begin(label, kind):
            idx = _pi[0]
            _pi[0] += 1
            self._begin_phase(task_id, idx, label, kind)
            return idx

        def end(idx, conclusion='', body=None):
            self._end_phase(task_id, idx, conclusion, body)

        def _cli(label, prompt, max_turns):
            """运行一次 CLI 阶段：自行打开一个 phase，正文随 token 流式填充该阶段气泡，
            失败/取消时直接收尾并返回取消标记。返回 (phase_index, reply, cancelled)。"""
            idx = begin(label, 'cli')
            _model = (task.get('extra') or {}).get('model') or None
            reply, _, err, rc, cancelled = self._run_cli(prompt, task_id, max_turns, model=_model)
            reply = _strip_control_lines(reply)
            if cancelled:
                self._set_status(task_id, self.STATUS_CANCELLED, error='已取消')
                self._emit(task_id, 'error', '任务已取消')
                self._finish_emit(task_id, 'cancelled')
                return idx, None, True
            if _is_auth_error(err or ''):
                self._set_status(task_id, self.STATUS_FAILED, error='CodeBuddy 认证失败')
                self._emit(task_id, 'error', 'CodeBuddy 认证失败，请在凭证保险库配置 codebuddy token 或执行 codebuddy /login')
                self._finish_emit(task_id, 'failed')
                return idx, None, True
            if rc not in (0, None):
                self._set_status(task_id, self.STATUS_FAILED, error='AI 执行出错（退出码 %s）' % rc)
                self._emit(task_id, 'error', 'AI 执行出错（退出码 %s）' % rc)
                self._finish_emit(task_id, 'failed')
                return idx, None, True
            return idx, reply, False

        # ---- 阶段 1：工作流选择（替代旧「意图分析」）----
        extra = task.get('extra') or {}
        wf_id = extra.get('workflow_id')
        if not wf_id:
            wf_id = self._resolve_workflow(task['prompt'], engine)
        wf = engine.get(wf_id)
        wf_id = wf['id']
        self._emit(task_id, 'workflow', {
            'id': wf_id, 'name': wf.get('name', wf_id),
            'icon': wf.get('icon', ''), 'color': wf.get('color', ''),
        })
        idx = begin('工作流选择', 'host')
        end(idx, conclusion='%s %s' % (wf.get('icon', ''), wf.get('name', wf_id)))

        # ---- start 步：shell 检查 + ask 提问（ask 会挂起等待用户选择）----
        repo = _project_root()
        hit_shells = {}
        answers = {}
        prev_issue = None  # resume 选中的具体单号
        plan_mode = bool(extra.get('plan_mode'))
        for step in engine.steps_of(wf, 'start'):
            if step.get('kind') == 'shell':
                if plan_mode:
                    # plan 模式：绝不执行任何 shell 命令，从源头杜绝代码改动
                    continue
                hit, _ = engine.run_shell_step(step, cwd=repo)
                if hit:
                    hit_shells[step.get('id')] = True
            elif step.get('kind') == 'ask':
                choice = self._run_ask_step(task_id, step, engine)
                if choice:
                    answers[step.get('id')] = choice
                    if step.get('id') == 'pick_ticket' and choice not in ('新开', '__list_tickets__'):
                        prev_issue = choice

        # ---- 阶段 2（AI 处理）：编译工作流提示并单次执行 ----
        workflow_prompt = engine.compile_prompt(wf, answers=answers, hit_shells=hit_shells)
        if wf_id == 'resume' and prev_issue:
            workflow_prompt = (workflow_prompt + '\n（本条为「继续」：你正在延续 %s 的处理，'
                                '请基于该单已有上下文继续推进，不要当成全新任务。）' % prev_issue)
        ci, reply, cancelled = _cli('AI 处理',
            self._build_prompt(task['prompt'], workflow_prompt=workflow_prompt, prev_issue=prev_issue), 50)
        if cancelled:
            return
        if not reply:
            reply = _PLACEHOLDER_AI_EMPTY
        reply, filed_id = _maybe_file_feedback(reply)
        end(ci, body=reply, conclusion='AI 已生成回复')

        # ---- Plan 模式：AI 仅产出计划文档，绝不改代码、不建单 ----
        if extra.get('plan_mode'):
            try:
                if plan_manager is None:
                    raise RuntimeError('plan_manager 未加载')
                # 从用户消息中提取标题（首行截断）
                title = (task['prompt'] or '').strip().splitlines()
                title = title[0][:40] if title else '未命名计划'
                plan_id = plan_manager.save_plan(
                    reply, owner_id=task.get('owner_id'), title=title,
                    status=plan_manager.STATUS_DRAFT)
                if plan_id:
                    idx = begin('生成计划文档', 'host')
                    end(idx, conclusion='已生成计划，可在「计划」面板查看与点评')
                    self._set_track_id(task_id, plan_id)
            except Exception as e:
                idx = begin('生成计划文档', 'host')
                end(idx, conclusion='计划文档写入失败：%s' % e)
            self._finish_completed(task_id)
            return

        # ---- end 步：复查 git 等 ----
        s_post = ''
        for step in engine.steps_of(wf, 'end'):
            if step.get('kind') == 'shell':
                hit, _ = engine.run_shell_step(step, cwd=repo)
                if hit:
                    s_post = '做事后检查：git 工作区仍有未提交改动，请复查并清理/提交后再结束'
        if s_post:
            idx = begin('收尾核查（git 仓库状态）', 'host')
            end(idx, conclusion=s_post)

        # ---- 建单 / 续写 ----
        is_code = wf_id in ('defect', 'suggest')
        answered_create = any(v == '建单' for v in answers.values())
        make_ticket = bool(answered_create) or (is_code and self._git_has_new_commit(repo))
        if prev_issue and wf_id == 'resume':
            # 续写用户选定的既有单：把分析与解决以「自动助手」身份追加回复。
            idx = begin('续写既有处理跟踪单（待验证）', 'host')
            a_txt = ''
            r_txt = (reply or '').strip()
            if r_txt.startswith('（任务已执行完成'):
                r_txt = ''
            ok_r = _add_feedback_comment(prev_issue, r_txt) if r_txt else True
            track_id = prev_issue
            self._set_track_id(task_id, track_id)
            s_ticket = ('已在反馈单 %s 中续写解决说明（自动助手身份）' % prev_issue) if ok_r \
                else ('（已尝试在反馈单 %s 中续写，但反馈中心暂不可达，将自动重试）' % prev_issue)
            end(idx, conclusion=s_ticket)
        elif make_ticket and not filed_id:
            idx = begin('创建处理跟踪单（待验证）', 'host')
            ref_id = _extract_ref_issue(task['prompt'])
            if ref_id:
                a_txt = ''
                r_txt = (reply or '').strip()
                if _is_placeholder_text(r_txt):
                    r_txt = ''
                ok_r = _add_feedback_comment(ref_id, r_txt) if r_txt else True
                track_id = ref_id
                self._set_track_id(task_id, track_id)
                s_ticket = '已在反馈单 #%s 中以「自动助手」身份回复了解决说明' % ref_id if ok_r \
                    else '（已尝试在反馈单 #%s 中回复，但反馈中心暂不可达，将自动重试）' % ref_id
            else:
                override_title = ('续接 %s' % prev_issue) if prev_issue else None
                _, track_id, s_ticket = _maybe_create_tracking_ticket(
                    task_id, task['prompt'], task.get('owner_id'),
                    reply, filed_id, None, git_clean=not hit_shells, intent=wf_id,
                    analysis=workflow_prompt, resolution=reply, title=override_title)
                if track_id:
                    self._set_track_id(task_id, track_id)
            end(idx, conclusion=s_ticket)

        self._finish_completed(task_id)

    # ---------- 工作流辅助 ----------
    def _resolve_workflow(self, message, engine):
        """实时推断工作流 id：先关键词确定性分类，再校验 auto_infer；
        不可推断（auto_infer=false）或落空时回落 chat。"""
        from workflow_engine import DEFAULT_WORKFLOW
        intent = _classify_intent(message)  # defect / suggestion / continue / chat
        if intent == 'continue':
            # 「继续」默认手动，不自动选（除非用户显式点选）
            return DEFAULT_WORKFLOW if not engine.get('resume').get('auto_infer', False) else 'resume'
        if intent in ('defect', 'suggestion'):
            wid = 'defect' if intent == 'defect' else 'suggest'
            if engine.get(wid).get('auto_infer', False):
                return wid
        return DEFAULT_WORKFLOW

    def _git_has_new_commit(self, repo):
        """粗略判断：仓库 HEAD 与远端/track 存在差异，或存在未推送提交，即认为有「新提交」。
        这里用「是否存在未推送到 origin 的提交」近似（git cherry）。"""
        try:
            out = subprocess.run('git rev-list --count --left-right @{upstream}...HEAD',
                                 shell=True, cwd=repo, capture_output=True, text=True, timeout=15)
            if out.returncode == 0:
                right = out.stdout.split('\t')[-1].strip()
                return right not in ('', '0')
        except Exception:
            pass
        return False

    def _run_ask_step(self, task_id, step, engine):
        """执行 ask 步骤：向 UI 下发提问，挂起等待用户选择，返回 choice 字符串。"""
        options = list(step.get('options', []))
        # resume 的「列出任务」选项：用最近未完成/失败的任务填充
        if '__list_tickets__' in options:
            recents = self._recent_unfinished_tasks(limit=8)
            options = [t for t in options if t != '__list_tickets__']
            options = recents + options
        event = threading.Event()
        self._answer_events[task_id] = event
        self._pending_answers.setdefault(task_id, {})
        self._emit(task_id, 'ask', {
            'step_id': step.get('id'),
            'question': step.get('question', '请选择'),
            'options': options,
        })
        # 挂起：最多等 30 分钟
        answered = event.wait(timeout=1800)
        choice = self._pending_answers.get(task_id, {}).get(step.get('id'))
        self._answer_events.pop(task_id, None)
        if not answered or not choice:
            return None
        return choice

    def answer_task(self, task_id, step_id, choice):
        """前端对用户 ask 的应答：写入答案并唤醒挂起的 worker。"""
        with self._lock:
            self._pending_answers.setdefault(task_id, {})[step_id] = choice
        ev = self._answer_events.get(task_id)
        if ev:
            ev.set()
        self._emit(task_id, 'ask_result', {'step_id': step_id, 'choice': choice})
        return True

    def _recent_unfinished_tasks(self, limit=8):
        """返回最近未完成/失败的任务提示，供 resume 选择续接。"""
        with self._lock:
            rows = self._db.execute(
                "SELECT task_id, prompt, status FROM ai_assistant_tasks "
                "WHERE status IN (?,?,?) ORDER BY created_at DESC LIMIT ?",
                (self.STATUS_PENDING, self.STATUS_RUNNING, self.STATUS_FAILED, limit)
            ).fetchall()
        return ['#%s %s' % (r['task_id'][:10], (r['prompt'] or '')[:24]) for r in rows]

    def _finish_completed(self, task_id):
        """把当前阶段日志组装为分阶段回复并标记完成、下发 done、结束 SSE。"""
        with self._lock:
            phases = [dict(p) for p in self._phase_log.get(task_id, [])]
        # 存库的最终回复：以各阶段「正文（body，AI 真实产出）优先」，仅当正文为空时
        # 才回退到「结论（conclusion，阶段短标签）」。此前用 conclusion or body 会把
        # 「AI 已生成回复」这类固定结论覆盖掉真实长文，导致对话里只见阶段标题、正文丢失
        # （前端表现为「任务已执行完成，无文本输出」占位）。
        final_reply = '\n\n'.join(
            (p.get('body') or p.get('conclusion') or '').strip()
            for p in phases if (p.get('body') or p.get('conclusion')))
        self._set_status(task_id, self.STATUS_COMPLETED, reply=final_reply,
                         phases=json.dumps(phases, ensure_ascii=False))
        self._emit(task_id, 'done', final_reply)
        self._finish_emit(task_id, 'completed')

    # ---------- SSE 发布订阅 ----------
    def _append_token(self, task_id, piece):
        """追加一个 token：写入缓冲区，并（若存在当前阶段）归属到该阶段气泡的正文，
        同时推送给所有订阅者（token 与 phase_chunk 各一份，前端以前者做保底、以后者填充气泡）。"""
        with self._lock:
            self._buffers.setdefault(task_id, []).append(piece)
            idx = self._cur_phase.get(task_id)
            if idx is not None:
                for p in self._phase_log.get(task_id, []):
                    if p['index'] == idx:
                        p['body'] += piece
                        break
            subs = list(self._subscribers.get(task_id, []))
        for q in subs:
            try:
                q.put(('token', piece))
                if idx is not None:
                    q.put(('phase_chunk',
                           json.dumps({'index': idx, 'text': piece}, ensure_ascii=False)))
            except Exception:
                pass

    def _append_thinking(self, task_id, piece):
        """追加一段思考增量：归属到当前 CLI 阶段气泡的 thinking 字段，并实时推送给订阅者，
        供前端「边想边显示」。思考不进 token 缓冲（不会混入最终正文）。"""
        if not piece:
            return
        with self._lock:
            idx = self._cur_phase.get(task_id)
            if idx is not None:
                for p in self._phase_log.get(task_id, []):
                    if p['index'] == idx:
                        p['thinking'] = (p.get('thinking') or '') + piece
                        break
            subs = list(self._subscribers.get(task_id, []))
        for q in subs:
            try:
                q.put(('thinking', json.dumps({'index': idx, 'text': piece},
                                              ensure_ascii=False)))
            except Exception:
                pass

    def _emit(self, task_id, etype, data):
        with self._lock:
            subs = list(self._subscribers.get(task_id, []))
        for q in subs:
            try:
                q.put((etype, data))
            except Exception:
                pass

    # ---- 阶段气泡事件（结构层面，不依赖提示词）----
    # 每个处理阶段 = 聊天窗口里的一个独立气泡。阶段开始发射 phase（state=running），
    # CLI 阶段的正文随 token 走 phase_chunk 流式填充，阶段结束发射 phase（state=done，
    # 含 conclusion 一句结论）。这样「一条命令」会被拆成多个可见气泡，且每阶段结束都
    # 回一句结论，而非挤在一个气泡里加进度条。
    def _begin_phase(self, task_id, index, label, kind):
        phase = {'index': index, 'label': label, 'kind': kind,
                 'state': 'running', 'conclusion': '', 'body': '', 'thinking': ''}
        with self._lock:
            self._phase_log.setdefault(task_id, []).append(phase)
            self._cur_phase[task_id] = index
        self._emit(task_id, 'phase',
                   json.dumps({'index': index, 'label': label, 'kind': kind,
                               'state': 'running'}, ensure_ascii=False))

    def _end_phase(self, task_id, index, conclusion='', body=None):
        label = ''
        with self._lock:
            for p in self._phase_log.get(task_id, []):
                if p['index'] == index:
                    p['state'] = 'done'
                    label = p.get('label', '')
                    if conclusion is not None:
                        p['conclusion'] = conclusion
                    if body is not None:
                        p['body'] = body
                    break
            self._cur_phase.pop(task_id, None)
        payload = {'index': index, 'label': label, 'state': 'done',
                   'conclusion': conclusion or ''}
        if body is not None:
            payload['body'] = body
        self._emit(task_id, 'phase', json.dumps(payload, ensure_ascii=False))

    def _finish_emit(self, task_id, _status):
        """通知所有订阅者任务结束（推送终止哨兵）并清理缓冲区。"""
        with self._lock:
            subs = list(self._subscribers.get(task_id, []))
            self._subscribers.pop(task_id, None)
            self._buffers.pop(task_id, None)
            self._phase_log.pop(task_id, None)
            self._cur_phase.pop(task_id, None)
            self._procs.pop(task_id, None)
        for q in subs:
            try:
                q.put(('__end__', ''))
            except Exception:
                pass

    def subscribe(self, task_id):
        """返回一个生成 SSE 文本块的生成器，按 task_id 订阅该任务的流式输出。

        - 已完成/失败/取消：立即回放最终结果并结束（支持刷新重连后直接拿到完整回复）；
        - 排队中（pending）：先下发 queued 事件，再等待被 worker 取出后推送 running 与 token；
        - 正在处理（running）：先回放已产出的阶段气泡日志（self._phase_log），再续接后续实时事件。
        """
        task = self.get_task(task_id)
        if task is None:
            yield _sse_block('error', '任务不存在')
            return

        status = task['status']
        if status == self.STATUS_COMPLETED:
            yield _sse_block('done', task['reply'] or '')
            return
        if status == self.STATUS_FAILED:
            yield _sse_block('error', task['error'] or '执行失败')
            return
        if status == self.STATUS_CANCELLED:
            yield _sse_block('error', '任务已取消')
            return

        # pending 或 running：注册订阅者
        q = queue.Queue()
        with self._lock:
            self._subscribers.setdefault(task_id, []).append(q)
            phases = [dict(p) for p in self._phase_log.get(task_id, [])]
            cur = self._db.execute(
                'SELECT status FROM ai_assistant_tasks WHERE task_id=?', (task_id,)).fetchone()
            cur_status = cur['status'] if cur else status

        if cur_status == self.STATUS_PENDING:
            yield _sse_block('queued', '')

        # 先重放持久化缓冲的「阶段气泡」日志：每个阶段按当前状态（running/done）
        # 整体下发，前端据 index 重建为独立气泡。这些阶段可能在 SSE 连上前就已发射，
        # 此前只重放 token 导致早期阶段被静默丢弃，表现为聊天窗口长期只显示「正在处理」。
        # 以 self._phase_log 为唯一可信源重建，根治该问题。
        for p in phases:
            payload = {'index': p['index'], 'label': p.get('label', ''),
                       'kind': p.get('kind', ''), 'state': p.get('state', 'done'),
                       'conclusion': p.get('conclusion', '')}
            if p.get('body'):
                payload['body'] = p['body']
            yield _sse_block('phase', json.dumps(payload, ensure_ascii=False))

        idle = 0
        while True:
            try:
                item = q.get(timeout=15)
            except queue.Empty:
                # 心跳保活，避免代理断开长连接；同时设上限，防止 worker 异常卡死导致连接永生
                idle += 1
                if idle > 40:  # 约 10 分钟无进展则主动断开
                    return
                yield ': keepalive\n\n'
                continue
            etype, data = item
            if etype == '__end__':
                return
            if etype == 'phase':
                yield _sse_block('phase', data)
            elif etype == 'phase_chunk':
                yield _sse_block('phase_chunk', data)
            elif etype == 'token':
                yield _sse_block('token', data)
            elif etype == 'thinking':
                yield _sse_block('thinking', data)
            elif etype == 'status':
                yield _sse_block('status', data)
            elif etype == 'queued':
                yield _sse_block('queued', '')
            elif etype == 'done':
                yield _sse_block('done', data)
                return
            elif etype == 'error':
                yield _sse_block('error', data)
                return

    # ---------- 列表 / 历史 ----------
    def list_tasks(self, history_limit=10):
        """返回 pending（FIFO 正序）+ active（running 的任务，含当前缓冲）+ 最近 history。"""
        with self._lock:
            pending_rows = self._db.execute(
                'SELECT task_id, prompt, status, created_at FROM ai_assistant_tasks '
                'WHERE status=? ORDER BY created_at ASC', (self.STATUS_PENDING,)).fetchall()
            active_rows = self._db.execute(
                'SELECT task_id, prompt, status, created_at FROM ai_assistant_tasks '
                'WHERE status=? ORDER BY created_at DESC LIMIT 1', (self.STATUS_RUNNING,)).fetchall()
            hist_rows = self._db.execute(
                "SELECT task_id, prompt, reply, status, error, phases, created_at, head_before FROM ai_assistant_tasks "
                "WHERE status IN (?, ?, ?) ORDER BY created_at DESC LIMIT ?",
                (self.STATUS_COMPLETED, self.STATUS_FAILED, self.STATUS_CANCELLED,
                 history_limit + 1)).fetchall()

        pending = [{'id': r['task_id'], 'prompt': r['prompt'], 'status': r['status'],
                    'created_at': r['created_at']} for r in pending_rows]

        active = None
        if active_rows:
            r = active_rows[0]
            with self._lock:
                buf = ''.join(self._buffers.get(r['task_id'], []))
                phases = [dict(p) for p in self._phase_log.get(r['task_id'], [])]
            active = {'id': r['task_id'], 'prompt': r['prompt'], 'status': r['status'],
                      'created_at': r['created_at'], 'stream': buf, 'phases': phases}

        has_more = len(hist_rows) > history_limit
        hist_rows = hist_rows[:history_limit]
        history = [{'id': r['task_id'], 'prompt': r['prompt'], 'reply': r['reply'],
                    'status': r['status'], 'error': r['error'],
                    'phases': _parse_phases(r['phases']),
                    'head_before': (r['head_before'] or '').strip(),
                    'created_at': r['created_at']} for r in hist_rows]

        return {'pending': pending, 'active': active, 'history': history, 'has_more': has_more}

    def history_page(self, cursor=None, limit=10):
        """分页获取更早的历史（按 created_at 倒序）。cursor 为上一页最后一条的 created_at。"""
        with self._lock:
            if cursor is not None:
                rows = self._db.execute(
                    "SELECT task_id, prompt, reply, status, error, phases, created_at, head_before FROM ai_assistant_tasks "
                    "WHERE status IN (?, ?, ?) AND created_at < ? ORDER BY created_at DESC LIMIT ?",
                    (self.STATUS_COMPLETED, self.STATUS_FAILED, self.STATUS_CANCELLED,
                     float(cursor), limit + 1)).fetchall()
            else:
                rows = self._db.execute(
                    "SELECT task_id, prompt, reply, status, error, phases, created_at, head_before FROM ai_assistant_tasks "
                    "WHERE status IN (?, ?, ?) ORDER BY created_at DESC LIMIT ?",
                    (self.STATUS_COMPLETED, self.STATUS_FAILED, self.STATUS_CANCELLED,
                     limit + 1)).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = [{'id': r['task_id'], 'prompt': r['prompt'], 'reply': r['reply'],
                  'status': r['status'], 'error': r['error'],
                  'head_before': (r['head_before'] or '').strip(),
                  'created_at': r['created_at']}
                 for r in rows]
        return {'history': items, 'has_more': has_more,
                'next_cursor': items[-1]['created_at'] if (items and has_more) else None}

    # ---------- 改动查询 / 回滚 ----------
    def get_task_changes(self, task_id):
        """返回任务自开始（head_before）到当前的代码改动摘要，供 UI 展示与回滚决策。

        返回 {success, head_before, head_after, commits:[str...], dirty:[str...], has_changes:bool}。
        无回滚基线（head_before 为空）时 has_changes 仍按当前仓库真实状态给出，但 message 提示无法回滚。
        """
        task = self.get_task(task_id)
        if not task:
            return {'success': False, 'message': '任务不存在'}
        repo = _project_root()
        head_before = (task.get('head_before') or '').strip()
        head_after = _git_rev_head(repo)
        commits = []
        if head_before and head_after and head_before != head_after:
            try:
                out = subprocess.run(['git', 'log', '--oneline',
                                       head_before + '..' + head_after],
                                     cwd=repo, capture_output=True, text=True, timeout=30).stdout
                commits = [l for l in out.splitlines() if l.strip()]
            except Exception:
                pass
        dirty = sorted(_git_dirty_files(repo) or set())
        return {'success': True, 'head_before': head_before, 'head_after': head_after,
                'commits': commits, 'dirty': dirty,
                'has_changes': bool(commits) or bool(dirty),
                'message': '' if head_before else '无回滚基线（任务开始前未记录 HEAD，无法撤销）'}

    def rollback_task(self, task_id):
        """停止/完成后「撤销本次改动」：把仓库回退到任务开始时的 HEAD（head_before）。

        这是破坏性操作（git reset --hard + git clean -fd），仅在任务已离开运行/排队态
        （completed / failed / cancelled）时允许，且需要合法的回滚基线哈希。返回 {success, ...}。
        """
        task = self.get_task(task_id)
        if not task:
            return {'success': False, 'message': '任务不存在'}
        status = task['status']
        if status in (self.STATUS_RUNNING, self.STATUS_PENDING):
            return {'success': False, 'message': '任务仍在处理/排队中，请先停止'}
        head_before = (task.get('head_before') or '').strip()
        if not head_before:
            return {'success': False, 'message': '无回滚基线，无法撤销本次改动'}
        if not re.match(r'^[0-9a-f]{7,40}$', head_before):
            return {'success': False, 'message': '回滚基线非法，拒绝执行'}
        repo = _project_root()
        try:
            r1 = subprocess.run(['git', 'reset', '--hard', head_before],
                                cwd=repo, capture_output=True, text=True, timeout=60)
            if r1.returncode != 0:
                return {'success': False, 'message': 'git reset --hard 失败：' + (r1.stderr or '').strip()[:300]}
            subprocess.run(['git', 'clean', '-fd'], cwd=repo,
                           capture_output=True, text=True, timeout=60)
            after = self.get_task_changes(task_id)
            return {'success': True, 'commits': after.get('commits', []),
                    'dirty': after.get('dirty', []),
                    'message': '已撤销本次改动，仓库回到任务开始前状态'}
        except Exception as e:
            return {'success': False, 'message': '回滚失败：' + str(e)}

    # ---------- 删除 / 取消 ----------
    def delete_task(self, task_id):
        """删除/取消一个 AI 任务。

        - pending：直接从队列移除并标记 cancelled（取消排队）；
        - running：置取消标记并 kill 子进程；
        - 终态：直接从历史中删除。

        返回 True 成功 / False 不存在 / None 已取消中。
        """
        task = self.get_task(task_id)
        if not task:
            return False
        status = task['status']
        if status == self.STATUS_PENDING:
            with self._lock:
                self._skip.add(task_id)
            self._set_status(task_id, self.STATUS_CANCELLED, error='已取消')
            self._emit(task_id, 'error', '任务已取消')
            self._finish_emit(task_id, 'cancelled')
            self._remove_from_unified(task_id)
            return True
        if status == self.STATUS_RUNNING:
            with self._lock:
                self._cancel[task_id] = True
                proc = self._procs.get(task_id)
                if proc and proc.poll() is None:
                    # 必须杀掉整个进程树（含子进程/孙进程），否则 CLI 拉起的 node 孙进程
                    # 仍持有 stdout 管道并继续运行：_run_cli 的读取循环不会结束、_process 挂起、
                    # 单 worker 线程卡死，表现为「任务还在跑、后续任务排队不动」。
                    self._terminate(proc)
            self._emit(task_id, 'error', '任务已取消')
            # worker 会在进程退出（管道关闭、读取循环结束）后将状态置为 cancelled 并结束 SSE
            return True
        # 终态：直接删除记录
        with self._lock:
            self._db.execute('DELETE FROM ai_assistant_tasks WHERE task_id=?', (task_id,))
            self._db.commit()
        self._remove_from_unified(task_id)
        return True

    def clear(self):
        """清空全部对话（含排队与历史），重置上下文。"""
        with self._lock:
            # 取消进行中的任务
            rows = self._db.execute(
                'SELECT task_id, status FROM ai_assistant_tasks WHERE status IN (?, ?)',
                (self.STATUS_PENDING, self.STATUS_RUNNING)).fetchall()
            for r in rows:
                if r['status'] == self.STATUS_PENDING:
                    self._skip.add(r['task_id'])
                else:
                    self._cancel[r['task_id']] = True
                    proc = self._procs.get(r['task_id'])
                    if proc and proc.poll() is None:
                        self._terminate(proc)
            self._db.execute('DELETE FROM ai_assistant_tasks')
            self._db.commit()
            self._subscribers.clear()
            self._buffers.clear()
        # 同步清理统一任务表中的 ai_assistant 镜像
        if self._ut:
            try:
                if self._tasks is not None:
                    # 插件代理以自身 kind 维度注册，clear 时无需逐项回查统一表
                    return True
                from shared.unified_tasks import get_tasks as ut_get_all
                for t in ut_get_all(role='admin', limit=200):
                    if (t.get('kind') == 'ai_assistant') or str(t.get('task_id', '')).startswith('ai:'):
                        self._remove_from_unified(t['task_id'].split(':', 1)[-1] if str(t['task_id']).startswith('ai:') else t['task_id'])
            except Exception:
                pass
        return True


# 单例
ai_mgr = AIChatManager()
# 供模块级函数（如 _load_codebuddy_token）访问单例的宿主注入依赖
_ai_mgr_singleton = ai_mgr
