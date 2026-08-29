# -*- coding: utf-8 -*-
"""验证 AI 回复里的宿主控制备注被剥离、空输出占位符识别正确。

背景：buddy CLI 在 --input-format text 下无 system 角色隔离，模型可能把
「（系统初步判定本条用户意图为：…）」「【本阶段任务：…】」等仅供内部判断的
控制备注回显进回复；此前这些备注直接进气泡/存库，且空输出被误报为「已完成」。
本测试确认 _strip_control_lines 能剔除这些回显行、_is_placeholder_text 能识别
空输出占位提示（含新旧两种文案）。
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, '..', 'src', 'extensions_host'))

import codebuddy as ac


def test_strip_intent_hint_line():
    # 模型把意图提示整行回显
    out = '（系统初步判定本条用户意图为：缺陷，供你参考）\n这是分析内容。'
    assert ac._strip_control_lines(out) == '这是分析内容。'


def test_strip_phase_task_line():
    # 模型回显「本阶段任务」控制行（execute 阶段单行格式）
    out = ('【本阶段任务：执行修改】以下为上一阶段（分析定位）的结论，请直接据此执行修改：\n'
           '我修改了 a.py。')
    assert ac._strip_control_lines(out) == '我修改了 a.py。'


def test_strip_continue_note_line():
    out = '（本条为「继续」意图：你正在延续反馈单 #202608130001 的处理）\n继续说明。'
    assert ac._strip_control_lines(out) == '继续说明。'


def test_strip_keeps_real_content():
    # 正常回复（含普通括号/方括号文本）不应被误删
    out = '问题根因在 codebuddy.py:918。请查看（系统文档）与【配置项】说明。'
    assert ac._strip_control_lines(out) == out


def test_strip_empty():
    assert ac._strip_control_lines('') == ''
    assert ac._strip_control_lines('   \n  ') == ''


def test_is_placeholder_text_old_and_new():
    assert ac._is_placeholder_text('（任务已执行完成，无文本输出）') is True
    assert ac._is_placeholder_text(ac._PLACEHOLDER_AI_EMPTY) is True
    assert ac._is_placeholder_text('这是正常的回复内容') is False
    assert ac._is_placeholder_text('') is False
