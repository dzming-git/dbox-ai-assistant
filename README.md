# dbox-codebuddy

DBox 平台的 **CodeBuddy** 扩展插件（纯插件，解压即用，对框架零入侵）。

> 原名 `ai_chat`，已于插件独立化时重命名为 `codebuddy`。
> 相关路径 / 路由 / 数据库表名等均已随之更新：
> - 插件 id：`codebuddy`
> - 后端路由前缀：`/api/ext/codebuddy`
> - 独立全屏路由：`/codebuddy`（由宿主框架按 manifest 的 `ui.standalone_route` 动态注册）
> - 任务表 kind：`codebuddy`

## 目录结构

```
.
├── manifest.json          # 插件元信息 + 能力声明（入口）
├── backend/               # 插件后端（create_blueprint(host) 工厂）
│   ├── server.py          # Blueprint 工厂，注册 /api/ext/codebuddy/* 路由
│   ├── engine.py          # 对话引擎、SSE 流式、任务表镜像
│   ├── plan_manager.py    # 方案（plan）管理
│   └── workflow_engine.py # 工作流步骤引擎
├── ui/
│   └── panel.html         # iframe 入口（悬浮球 / 全屏页共用）
├── workflows/             # 配置驱动的工作流定义（chat/defect/suggest/resume/plan）
│   ├── chat.yaml
│   ├── defect.yaml
│   ├── suggest.yaml
│   ├── resume.yaml
│   └── plan.yaml
└── tests/                 # 插件自带测试
```

## 安装到 DBox

本插件是「纯插件」：不修改 DBox 框架任何源码，只需把本仓库内容放置到框架的插件目录即可。

1. 在 DBox 项目根目录下，将本仓库解压 / 克隆到 `extensions/codebuddy/`：

   ```bash
   # 方式一：直接拷贝
   cp -r dbox-codebuddy extensions/codebuddy

   # 方式二：作为可更新的独立仓库（推荐，便于 pull 更新）
   git clone https://github.com/dzming-git/dbox-codebuddy.git extensions/codebuddy
   ```

2. 重启扩展宿主服务（使框架扫描并加载插件）：

   ```bash
   nssm restart dbox-extensions
   ```

3. 框架会自动：
   - 扫描 `extensions/codebuddy/manifest.json`
   - 加载 `backend/server.py` 的 `create_blueprint(host)`，挂载 `/api/ext/codebuddy/*`
   - 按 `ui.standalone_route` 动态注册 `/codebuddy` 全屏路由
   - 在右下角挂悬浮球（按 `ui.mount: floating`）

4. 打开前端（`http://<host>:5173/`）即可看到悬浮球；点开或用 `/codebuddy` 进入全屏对话。

## 卸载

直接删除 `extensions/codebuddy/` 目录并重启 `dbox-extensions` 即可，框架自动跳过，无残留、无崩溃。

## 与框架的契约

插件只通过宿主框架注入的 `host` 对象通信（见 DBox 文档 `docs/development/plugin_architecture.md`），
**严禁** `import` 框架内部模块（`extensions_host.*` / `shared.*` / `web.*`）。后端能力（数据目录、
凭证、统一任务表、HTTP 客户端、鉴权装饰器）全部由 `host` 提供。

## 配置

在面板首次使用时，按 `ui.needs_credential`（codebuddy token）提示配置凭证；工作流可在
`workflows/*.yaml` 中自行增删。
