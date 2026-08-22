# -*- coding: utf-8 -*-
"""AI 助手 Plan 模式：计划文档（md）管理器。

设计：
- 每个计划是一个独立 md 文件，落盘于 data/ai-plans/<id>.md；
- 计划有状态：draft（草稿，AI 已生成待点评）/ approved（用户已确认满意）/
  executed（已提交执行、真正改代码）；
- 用户在 UI 点评（追加"用户点评"段），满意后点「执行」才触发实际代码改动；
- Plan 阶段 AI 只写 md、绝不调用 shell / git / 改代码（由工作流配置 + 本模块
  的存储职责共同保证：AI 在 plan 工作流下不被允许 shell 工具，计划内容以 md 文件
  形式落地，执行阶段才以 defect/suggest 工作流重新提交计划内容）。

与 ai_assistant.py 同进程运行，复用 extensions_host 的 data 目录约定。
"""
import os
import re
import time
import uuid
import threading

_PLAN_DIR = None
_LOCK = threading.RLock()

# 计划状态枚举
STATUS_DRAFT = 'draft'
STATUS_APPROVED = 'approved'
STATUS_EXECUTED = 'executed'


def init(data_dir):
    """初始化计划目录。data_dir 为 extensions_host 的 data 根目录。"""
    global _PLAN_DIR
    _PLAN_DIR = os.path.join(data_dir, 'ai-plans')
    try:
        os.makedirs(_PLAN_DIR, exist_ok=True)
    except Exception:
        pass


def _safe_id(plan_id):
    return re.sub(r'[^A-Za-z0-9_\-]', '', plan_id or '')


def _path(plan_id):
    pid = _safe_id(plan_id)
    if not pid:
        return None
    return os.path.join(_PLAN_DIR, pid + '.md')


def save_plan(content, owner_id=None, title=None, status=STATUS_DRAFT):
    """写一个新计划文档，返回 plan_id（不含扩展名）。"""
    if _PLAN_DIR is None:
        return None
    plan_id = 'plan_' + uuid.uuid4().hex[:12]
    created = time.time()
    head = (
        '---\n'
        'plan_id: %s\n'
        'status: %s\n'
        'owner_id: %s\n'
        'created_at: %s\n'
        'title: %s\n'
        '---\n\n'
        % (plan_id, status, owner_id or '', created, (title or '未命名计划').strip())
    )
    body = (content or '').strip() + '\n\n---\n\n## 用户点评\n\n'
    with _LOCK:
        try:
            with open(_path(plan_id), 'w', encoding='utf-8') as f:
                f.write(head + body)
        except Exception:
            return None
    return plan_id


def _parse_meta(text):
    meta = {}
    m = re.match(r'^---\s*\n(.*?)\n---\s*\n', text, re.DOTALL)
    if not m:
        return meta
    for line in m.group(1).splitlines():
        if ':' in line:
            k, v = line.split(':', 1)
            meta[k.strip()] = v.strip()
    return meta


def _split_comments(text):
    """把文档拆分为 (front_matter+正文, 点评段内容)。"""
    # 找到「## 用户点评」标题位置
    idx = text.find('\n## 用户点评')
    if idx == -1:
        return text, ''
    return text[:idx], text[idx:]


def list_plans():
    """返回计划列表摘要（不含全文）。"""
    if _PLAN_DIR is None or not os.path.isdir(_PLAN_DIR):
        return []
    out = []
    with _LOCK:
        for fn in sorted(os.listdir(_PLAN_DIR), reverse=True):
            if not fn.endswith('.md'):
                continue
            plan_id = fn[:-3]
            try:
                with open(os.path.join(_PLAN_DIR, fn), 'r', encoding='utf-8') as f:
                    text = f.read()
            except Exception:
                continue
            meta = _parse_meta(text)
            # 从正文中抽取首行非空作为预览
            body, _ = _split_comments(text)
            preview = ''
            for line in body.splitlines():
                line = line.strip()
                if line and not line.startswith('---') and not line.startswith('#'):
                    preview = line
                    break
            out.append({
                'plan_id': plan_id,
                'title': meta.get('title', '未命名计划'),
                'status': meta.get('status', STATUS_DRAFT),
                'owner_id': meta.get('owner_id', ''),
                'created_at': float(meta.get('created_at', 0) or 0),
                'preview': preview[:80],
            })
    return out


def get_plan(plan_id):
    """返回计划详情：{plan_id, title, status, content, comments, created_at}。"""
    path = _path(plan_id)
    if not path or not os.path.isfile(path):
        return None
    with _LOCK:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                text = f.read()
        except Exception:
            return None
    meta = _parse_meta(text)
    body, comment_block = _split_comments(text)
    # 正文 = front_matter 之后的部分（去掉 ## 用户点评 之前的内容里的 --- 头）
    content = re.sub(r'^---.*?---\s*\n', '', body, flags=re.DOTALL).strip()
    comments = []
    if comment_block:
        # 解析点评条目：- **用户**：xxx @ <unix时间戳>
        for m in re.finditer(r'-\s*\*\*用户\*\*：([\s\S]*?)\s+@\s+(\d{10,})', comment_block):
            comments.append({'text': m.group(1).strip(), 'ts': float(m.group(2))})
    return {
        'plan_id': plan_id,
        'title': meta.get('title', '未命名计划'),
        'status': meta.get('status', STATUS_DRAFT),
        'owner_id': meta.get('owner_id', ''),
        'created_at': float(meta.get('created_at', 0) or 0),
        'content': content,
        'comments': comments,
    }


def add_comment(plan_id, comment, owner_id=None):
    """追加一条用户点评到 md 文档的「用户点评」段。"""
    path = _path(plan_id)
    if not path or not os.path.isfile(path):
        return False
    comment = (comment or '').strip()
    if not comment:
        return False
    ts = int(time.time())
    entry = '- **用户**：%s @ %d\n' % (comment, ts)
    with _LOCK:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                text = f.read()
            if '\n## 用户点评' not in text and text.find('## 用户点评') == -1:
                text = text.rstrip() + '\n\n---\n\n## 用户点评\n\n'
            # 在「## 用户点评」段后追加
            if '## 用户点评' in text:
                text = text.rstrip() + '\n' + entry
            else:
                text = text.rstrip() + '\n' + entry
            with open(path, 'w', encoding='utf-8') as f:
                f.write(text)
        except Exception:
            return False
    return True


def set_status(plan_id, status):
    """更新计划 front_matter 中的 status 字段。"""
    path = _path(plan_id)
    if not path or not os.path.isfile(path):
        return False
    with _LOCK:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                text = f.read()
            text = re.sub(r'(status:\s*)([^\n]+)', lambda m: m.group(1) + status, text, count=1)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(text)
        except Exception:
            return False
    return True


def delete_plan(plan_id):
    path = _path(plan_id)
    if not path or not os.path.isfile(path):
        return False
    try:
        os.remove(path)
        return True
    except Exception:
        return False
