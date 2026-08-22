"""AI 助手插件后端入口：导出 create_blueprint(host)。

本模块是「纯插件」后端契约的实现示例：
- 不 import 任何框架内部包（extensions_host / shared / web / manager）；
- 仅通过 host 宿主对象访问框架能力（vault / tasks / login_required / data_dir）；
- 自身业务逻辑全部在 engine.py / workflow_engine.py / plan_manager.py（同目录）。
"""
from flask import Blueprint, request, jsonify, Response, stream_with_context
import os as _os
import sys as _sys

# 把 backend/ 自身目录加入 sys.path，使同目录模块（engine / plan_manager /
# workflow_engine）可用绝对 import 互引，避免插件包路径耦合问题。
_backend_dir = _os.path.dirname(_os.path.abspath(__file__))
if _backend_dir not in _sys.path:
    _sys.path.insert(0, _backend_dir)

# 同目录模块（插件自包含，不 import 框架内部包）
from engine import ai_mgr, list_models
from plan_manager import init as plan_init, get_plan, STATUS_EXECUTED, set_status, delete_plan


def create_blueprint(host):
    """由框架注入 host，构造并返回本插件的 Blueprint。"""
    bp = Blueprint('ext_ai_assistant', __name__, url_prefix=host.url_prefix)

    # 初始化插件自有管理器（数据目录 + 宿主注入的 vault/tasks 代理）
    try:
        ai_mgr.init(host.data_dir, vault=host.vault, tasks=host.tasks)
        plan_init(host.data_dir)
    except Exception as e:
        host.logger.error('AI 助手初始化失败: %s', e)

    # 把管理器句柄挂到 host.app_state，供后续请求复用
    host.app_state['mgr'] = ai_mgr

    @bp.route('', methods=['POST'])
    @bp.route('/enqueue', methods=['POST'])
    @host.login_required
    def enqueue():
        """入队一条用户消息，立即返回 task_id（不阻塞、不流式）。"""
        data = request.get_json(silent=True) or {}
        message = (data.get('message') or '').strip()
        if not message:
            return jsonify({'success': False, 'message': 'message 必填'}), 400
        wf_id = (data.get('workflow_id') or '').strip() or None
        manual = bool(data.get('manual'))
        plan_mode = bool(data.get('plan_mode', False))
        model = (data.get('model') or '').strip() or None
        task_id, err = ai_mgr.enqueue(message, g_user_id(), workflow_id=wf_id,
                                      manual=manual, plan_mode=plan_mode, model=model)
        if err:
            return jsonify({'success': False, 'message': err}), 400
        return jsonify({'success': True, 'task_id': task_id})

    @bp.route('/models', methods=['GET'])
    @host.login_required
    def models():
        return jsonify({'success': True, 'models': list_models()})

    @bp.route('/workflows', methods=['GET'])
    @host.login_required
    def workflows():
        try:
            return jsonify({'success': True, 'workflows': ai_mgr.list_workflows()})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    @bp.route('/tasks/<task_id>/answer', methods=['POST'])
    @host.login_required
    def answer(task_id):
        data = request.get_json(silent=True) or {}
        step_id = (data.get('step_id') or '').strip()
        choice = (data.get('choice') or '').strip()
        if not step_id or not choice:
            return jsonify({'success': False, 'message': 'step_id 与 choice 必填'}), 400
        ok = ai_mgr.answer_task(task_id, step_id, choice)
        if not ok:
            return jsonify({'success': False, 'message': '任务不存在或无需应答'}), 404
        return jsonify({'success': True})

    @bp.route('/tasks', methods=['GET'])
    @host.login_required
    def tasks():
        try:
            limit = int(request.args.get('limit', 10))
        except (TypeError, ValueError):
            limit = 10
        out = ai_mgr.list_tasks(history_limit=max(1, min(limit, 50)))
        return jsonify({'success': True, **out})

    @bp.route('/history', methods=['GET'])
    @host.login_required
    def history():
        try:
            limit = int(request.args.get('limit', 10))
        except (TypeError, ValueError):
            limit = 10
        cursor = request.args.get('cursor')
        if cursor:
            try:
                cursor = float(cursor)
            except (TypeError, ValueError):
                cursor = None
        out = ai_mgr.history_page(cursor=cursor, limit=max(1, min(limit, 50)))
        return jsonify({'success': True, **out})

    @bp.route('/tasks/<task_id>/stream', methods=['GET'])
    @host.login_required
    def stream(task_id):
        return Response(stream_with_context(ai_mgr.subscribe(task_id)),
                        mimetype='text/event-stream',
                        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

    @bp.route('/tasks/<task_id>', methods=['DELETE'])
    @host.login_required
    def delete(task_id):
        ok = ai_mgr.delete_task(task_id)
        if ok is None:
            return jsonify({'success': False, 'message': '任务取消中'}), 409
        if ok is False:
            return jsonify({'success': False, 'message': '任务不存在'}), 404
        return jsonify({'success': True})

    @bp.route('/tasks/<task_id>/changes', methods=['GET'])
    @host.login_required
    def changes(task_id):
        return jsonify(ai_mgr.get_task_changes(task_id))

    @bp.route('/tasks/<task_id>/rollback', methods=['POST'])
    @host.login_required
    def rollback(task_id):
        return jsonify(ai_mgr.rollback_task(task_id))

    @bp.route('/clear', methods=['POST'])
    @host.login_required
    def clear():
        ai_mgr.clear()
        return jsonify({'success': True})

    @bp.route('/resource-resolve', methods=['GET'])
    @host.login_required
    def resource_resolve():
        """解析 AI 回复中的资源引用。经 host.http 调用主服务内部接口，不 import 主业务代码。"""
        rtype = (request.args.get('type') or '').strip().lower()
        ref = (request.args.get('ref') or '').strip()
        if not ref:
            return jsonify({'success': True, 'found': False})
        try:
            url_root = host.config.get('main_service_url', 'http://127.0.0.1:8080')
            raw = host.http.get(
                url_root + '/internal/resource-resolve?type=%s&ref=%s' % (rtype, ref))
            import json as _json
            r = _json.loads(raw)
            if isinstance(r, dict) and r.get('success'):
                return jsonify(r)
        except Exception as e:
            host.logger.warning('resource-resolve 失败: %s', e)
        return jsonify({'success': True, 'found': False})

    # ---------- Plan 模式（计划文档，AI 仅生成 md，执行时再改代码） ----------
    @bp.route('/plans', methods=['GET'])
    @host.login_required
    def list_plans():
        try:
            return jsonify({'success': True, 'plans': get_plan_list()})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    @bp.route('/plans/<plan_id>', methods=['GET'])
    @host.login_required
    def get_plan_route(plan_id):
        plan = get_plan(plan_id)
        if plan is None:
            return jsonify({'success': False, 'message': '计划不存在'}), 404
        return jsonify({'success': True, 'plan': plan})

    @bp.route('/plans/<plan_id>/comment', methods=['POST'])
    @host.login_required
    def comment_plan(plan_id):
        data = request.get_json(silent=True) or {}
        ok = plan_manager_comment(plan_id, data.get('comment', ''))
        if not ok:
            return jsonify({'success': False, 'message': '计划不存在'}), 404
        return jsonify({'success': True})

    @bp.route('/plans/<plan_id>/execute', methods=['POST'])
    @host.login_required
    def execute_plan(plan_id):
        """用户确认计划后执行：把计划正文作为执行指令重新提交（AI 真正改代码）。"""
        plan = get_plan(plan_id)
        if plan is None:
            return jsonify({'success': False, 'message': '计划不存在'}), 404
        if plan['status'] == STATUS_EXECUTED:
            return jsonify({'success': False, 'message': '该计划已执行过'}), 400
        exec_msg = (
            '请基于以下已确认的修改计划，实际执行代码改动（可读取/修改文件、'
            '运行必要命令并验证）：\n\n' + plan['content']
        )
        data = request.get_json(silent=True) or {}
        model = (data.get('model') or '').strip() or None
        task_id, err = ai_mgr.enqueue(exec_msg, g_user_id(), workflow_id=None,
                                      manual=True, model=model)
        if err:
            return jsonify({'success': False, 'message': err}), 400
        set_status(plan_id, STATUS_EXECUTED)
        return jsonify({'success': True, 'task_id': task_id})

    @bp.route('/plans/<plan_id>', methods=['DELETE'])
    @host.login_required
    def delete_plan_route(plan_id):
        ok = delete_plan(plan_id)
        if not ok:
            return jsonify({'success': False, 'message': '计划不存在'}), 404
        return jsonify({'success': True})

    return bp


# 避免循环 import：从 plan_manager 惰性取列表/评论接口
def get_plan_list():
    from plan_manager import list_plans as _list
    return _list()


def plan_manager_comment(plan_id, comment):
    from plan_manager import comment_plan as _comment
    return _comment(plan_id, comment)


def g_user_id():
    """从 Flask 请求上下文读取当前用户（由 host.login_required 注入 g.user_id）。"""
    from flask import g
    return getattr(g, 'user_id', None)
