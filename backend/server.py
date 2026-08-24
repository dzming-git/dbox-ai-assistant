"""AI 助手插件后端入口（归零版）。

只提供纯聊天所需的最小接口集：
- POST '' / '/chat'  发送一条消息，返回 { task_id }
- GET  '/stream'     按 task_id 订阅 SSE 流式输出（token/thinking/assistant/done/error）
- GET  '/models'     可选模型列表
- GET  '/history'    当前用户的历史对话
- POST '/clear'      清空当前用户历史
- POST '/cancel'     取消某个进行中的任务
"""
from flask import Blueprint, request, jsonify, Response, stream_with_context

from .engine import ai_mgr, list_models


def create_blueprint(host):
    bp = Blueprint('ext_ai_assistant', __name__, url_prefix=host.url_prefix)

    try:
        ai_mgr.init(host.data_dir, vault=host.vault, tasks=host.tasks)
    except Exception as e:
        host.logger.error('AI 助手初始化失败: %s', e)

    @bp.route('', methods=['POST'])
    @bp.route('/chat', methods=['POST'])
    @host.login_required
    def chat():
        data = request.get_json(silent=True) or {}
        message = (data.get('message') or '').strip()
        if not message:
            return jsonify({'success': False, 'message': 'message 必填'}), 400
        model = (data.get('model') or '').strip() or None
        task_id, err = ai_mgr.chat(message, g_user_id(), model=model)
        if err:
            return jsonify({'success': False, 'message': err}), 400
        return jsonify({'success': True, 'task_id': task_id})

    @bp.route('/models', methods=['GET'])
    @host.login_required
    def models():
        return jsonify({'success': True, 'models': list_models()})

    @bp.route('/history', methods=['GET'])
    @host.login_required
    def history():
        limit = _int_arg('limit', 50)
        return jsonify({'success': True, 'history': ai_mgr.history(g_user_id(), limit=limit)})

    @bp.route('/clear', methods=['POST'])
    @host.login_required
    def clear():
        ai_mgr.clear(g_user_id())
        return jsonify({'success': True})

    @bp.route('/cancel', methods=['POST'])
    @host.login_required
    def cancel():
        data = request.get_json(silent=True) or {}
        task_id = (data.get('task_id') or '').strip()
        if not task_id:
            return jsonify({'success': False, 'message': 'task_id 必填'}), 400
        ok = ai_mgr.cancel(task_id)
        if not ok:
            return jsonify({'success': False, 'message': '任务不存在'}), 404
        return jsonify({'success': True})

    @bp.route('/stream', methods=['GET'])
    @host.login_required
    def stream():
        task_id = (request.args.get('task_id') or '').strip()
        if not task_id:
            return jsonify({'success': False, 'message': 'task_id 必填'}), 400
        return Response(stream_with_context(ai_mgr.subscribe(task_id)),
                        mimetype='text/event-stream',
                        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

    return bp


def _int_arg(name, default):
    try:
        return int(request.args.get(name, default))
    except (TypeError, ValueError):
        return default


def g_user_id():
    from flask import g
    return getattr(g, 'user_id', None)
