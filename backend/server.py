"""AI 助手插件后端入口（归零版）。

只提供纯聊天所需的最小接口集：
- POST '' / '/chat'          发送一条消息，返回 { task_id }（支持 conversation_id）
- GET  '/stream'             按 task_id 订阅 SSE 流式输出（token/thinking/assistant/done/error）
- GET  '/models'             可选模型列表
- GET  '/history'            当前用户/会话的历史对话（?conversation_id=）
- POST '/clear'              清空当前用户/会话历史（body 可带 conversation_id）
- POST '/cancel'             取消某个进行中的任务
- GET  '/conversations'      列出当前用户的会话
- POST '/conversations'      新建会话（body 可带 title），返回 { conversation_id }
- GET  '/conversations/<cid>/messages'  某会话的消息列表
- POST '/conversations/<cid>/rename'    重命名会话（body 带 title）
- DELETE '/conversations/<cid>'         删除会话
"""
from flask import Blueprint, request, jsonify, Response, stream_with_context

from .engine import ai_mgr, list_models


def create_blueprint(host):
    bp = Blueprint('ext_ai_assistant', __name__, url_prefix=host.url_prefix)

    # init 失败时记录完整 traceback（而非只记 message），便于定位为何引擎未就绪；
    # 并把异常暂存到 bp._init_error，供各路由在调用 ai_mgr 前给出明确的 JSON 错误
    # （避免 Flask 默认 500 HTML 页面导致前端 JSON.parse 报 “Unexpected token '<'”）。
    bp._init_error = None
    try:
        ai_mgr.init(host)
    except Exception as e:
        bp._init_error = 'AI 助手初始化失败: %s' % e
        host.logger.exception('AI 助手初始化失败（引擎将不可用）')

    def _engine_guard():
        """引擎未就绪时返回 JSON 错误响应，避免抛异常变成 HTML 500。"""
        if bp._init_error:
            return jsonify({'success': False, 'message': bp._init_error}), 503
        if not getattr(ai_mgr, '_db_path', None):
            return jsonify({'success': False, 'message': 'AI 引擎尚未初始化完成'}), 503
        return None

    @bp.route('', methods=['POST'])
    @bp.route('/chat', methods=['POST'])
    @host.login_required
    def chat():
        g = _engine_guard()
        if g is not None:
            return g
        try:
            data = request.get_json(silent=True) or {}
            message = (data.get('message') or '').strip()
            if not message:
                return jsonify({'success': False, 'message': 'message 必填'}), 400
            model = (data.get('model') or '').strip() or None
            conversation_id = (data.get('conversation_id') or '').strip() or None
            task_id, err = ai_mgr.chat(message, g_user_id(), model=model,
                                       conversation_id=conversation_id)
            if err:
                return jsonify({'success': False, 'message': err}), 400
            # 回传本次命中的会话 id，便于前端同步当前会话
            conv_id = ai_mgr._tasks.get(task_id, {}).get('conversation_id')
            return jsonify({'success': True, 'task_id': task_id,
                            'conversation_id': conv_id})
        except Exception as e:
            host.logger.exception('chat 处理失败')
            return jsonify({'success': False, 'message': '服务异常: %s' % e}), 500

    @bp.route('/models', methods=['GET'])
    @host.login_required
    def models():
        return jsonify({'success': True, 'models': list_models()})

    @bp.route('/history', methods=['GET'])
    @host.login_required
    def history():
        g = _engine_guard()
        if g:
            return g
        try:
            limit = _int_arg('limit', 50)
            conversation_id = (request.args.get('conversation_id') or '').strip() or None
            return jsonify({'success': True,
                            'history': ai_mgr.history(g_user_id(), conversation_id, limit=limit)})
        except Exception as e:
            host.logger.exception('history 加载失败')
            return jsonify({'success': False, 'message': '加载历史失败: %s' % e}), 500

    @bp.route('/clear', methods=['POST'])
    @host.login_required
    def clear():
        g = _engine_guard()
        if g is not None:
            return g
        data = request.get_json(silent=True) or {}
        conversation_id = (data.get('conversation_id') or '').strip() or None
        ai_mgr.clear(g_user_id(), conversation_id=conversation_id)
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

    @bp.route('/tasks', methods=['GET'])
    @host.login_required
    def tasks():
        g = _engine_guard()
        if g is not None:
            return g
        # 供框架层 ExtensionHost.vue 的 pollBusy 轻量轮询（忙碌态/未读角标）。
        return jsonify(ai_mgr.tasks_state(g_user_id()))

    # ---------- 会话管理 ----------
    @bp.route('/conversations', methods=['GET'])
    @host.login_required
    def conversations():
        g = _engine_guard()
        if g is not None:
            return g
        try:
            return jsonify({'success': True,
                            'conversations': ai_mgr.list_conversations(g_user_id())})
        except Exception as e:
            host.logger.exception('会话列表加载失败')
            return jsonify({'success': False, 'message': '加载会话失败: %s' % e}), 500

    @bp.route('/conversations', methods=['POST'])
    @host.login_required
    def conversations_create():
        g = _engine_guard()
        if g is not None:
            return g
        data = request.get_json(silent=True) or {}
        title = (data.get('title') or '').strip() or None
        try:
            cid = ai_mgr.create_conversation(g_user_id(), title=title)
            return jsonify({'success': True, 'conversation_id': cid})
        except Exception as e:
            host.logger.exception('创建会话失败')
            return jsonify({'success': False, 'message': '创建会话失败: %s' % e}), 500

    @bp.route('/conversations/<conversation_id>/messages', methods=['GET'])
    @host.login_required
    def conversation_messages(conversation_id):
        g = _engine_guard()
        if g is not None:
            return g
        try:
            limit = _int_arg('limit', 200)
            msgs = ai_mgr.history(g_user_id(), conversation_id, limit=limit)
            return jsonify({'success': True, 'messages': msgs})
        except Exception as e:
            host.logger.exception('会话消息加载失败')
            return jsonify({'success': False, 'message': '加载消息失败: %s' % e}), 500

    @bp.route('/conversations/<conversation_id>/rename', methods=['POST'])
    @host.login_required
    def conversation_rename(conversation_id):
        g = _engine_guard()
        if g is not None:
            return g
        data = request.get_json(silent=True) or {}
        title = (data.get('title') or '').strip()
        if not title:
            return jsonify({'success': False, 'message': 'title 必填'}), 400
        ok = ai_mgr.rename_conversation(g_user_id(), conversation_id, title)
        if not ok:
            return jsonify({'success': False, 'message': '会话不存在'}), 404
        return jsonify({'success': True})

    @bp.route('/conversations/<conversation_id>', methods=['DELETE'])
    @host.login_required
    def conversation_delete(conversation_id):
        g = _engine_guard()
        if g is not None:
            return g
        ok = ai_mgr.delete_conversation(g_user_id(), conversation_id)
        if not ok:
            return jsonify({'success': False, 'message': '会话不存在'}), 404
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


def __ext_busy__():
    """热重载保护钩子：AI 助手有正在跑/排队任务时返回 True。

    宿主热重载会在重新 import 本模块前征询此函数；返回 True 时宿主会
    延迟重载，避免正在生成的对话因 ai_mgr 单例被丢弃而断流。
    """
    try:
        return ai_mgr.is_busy()
    except Exception:
        return False
