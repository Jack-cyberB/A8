# A8 Monorepo (V2 Analysis Workspace)

A8 建筑能源智能管理系统：
- 前端：Vue3 + Element Plus + ECharts
- 后端：Python 轻量 API + 规则异常检测 + 闭环处理状态机
- 数据：BDG2 电力时序 + 司空知识条目

## 双知识源约定

当前项目知识问答固定分为两套知识源：

- `sikong`：场景运维库
  - 用于建筑场景、设备常识、运维经验、排查建议类问答
  - 上传包目录：`docs/ragflow/sikong-kb-pack/`
- `standard-kb`：标准规范库
  - 用于规范要求、定额标准、术语定义、合规依据类问答
  - 上传包目录：`docs/ragflow/standard-kb-pack/`

标准规范库首批纳入清单：

- `GB 50365-2019 空调通风系统运行管理标准`
- `GB 50736-2012 民用建筑供暖通风与空气调节设计规范`
- `GB 51348-2019 民用建筑电气设计标准`
- `GB/T 34913-2017 民用建筑能耗分类及表示方法`
- `GB 55015-2021 建筑节能与可再生能源利用通用规范`
- `GB/T 51140-2015 建筑节能基本术语标准`
- `GB/T 40571-2021 智能服务 预测性维护 通用要求`
- `DB37T 2671-2019 教育机构能源消耗定额标准`
- `DB37 T 5197-2021 公共建筑节能监测系统技术标准`

本轮明确排除：

- 热泵彩页
- 泵产品样本 PDF
- PNG 图片资料

## BDG2 展示建筑决策

当前项目的建筑展示策略固定为“前台少量代表建筑展示 + 后台全量同类样本对标”。

- 前台展示建筑固定为：
  - `Panther_education_Genevieve（教学楼）`
  - `Panther_education_Jerome（实验楼）`
  - `Panther_office_Patti（办公楼）`
  - `Panther_lodging_Marisol（宿舍）`
  - `Panther_assembly_Denice（体育馆）`
  - `Fox_public_Martin（图书馆）`
  - `Fox_food_Scott（食堂）`
- 这组建筑用于页面展示、AI 问答上下文和答辩讲述，不再直接使用“教学楼 / 办公楼 / 实验楼”这类纯演示占位名。
- 展示名称规则固定为：
  - `原始 building_id + （中文类别）`
  - 例如：`Panther_education_Genevieve（教学楼）`
- “同类型建筑比较”功能不以前台这 7 栋建筑互相比，而是仍然基于 BDG2 后台样本池计算。
- 对标池规则固定为：
  - 教学楼：`Education + Classroom` 同类样本
  - 实验楼：`Education + Laboratory / Research / Academic` 同类样本
  - 办公楼：`Office + Office` 同类样本
  - 宿舍：`Lodging/residential + Dormitory / Residence Hall` 同类样本
  - 图书馆：`Public services + Library` 同类样本
  - 体育馆：`Entertainment/public assembly + Gymnasium / Stadium / Fitness Center` 同类样本
  - 食堂：`Food sales and service` 同类样本
- 这样做的目标是同时满足：
  - 前端展示简洁，适合比赛录屏和答辩
  - 建筑身份保留 BDG2 原始来源
  - 同类对标仍基于完整样本池，结果有统计意义

如后续扩展水、空调、照明等维度，优先在这 7 栋展示建筑内扩展，不随意更换展示对象。

## 目录

- `frontend/` 页面与交互
- `backend/` 接口与规则逻辑
- `data/raw/` 原始数据
- `data/normalized/` 标准化数据与质量报告
- `data/runtime/` 运行态动作日志（自动生成）
- `scripts/` 数据和回归脚本
- `tests/e2e/` Playwright 冒烟回归

## 快速启动

```powershell
python scripts/validate_data_quality.py
python backend/server.py
```

浏览器访问 [http://127.0.0.1:8000](http://127.0.0.1:8000)

## 主要 API

- `GET /api/buildings`
- `GET /api/analysis/summary`
- `GET /api/analysis/trend`
- `GET /api/analysis/compare`
- `GET /api/analysis/distribution`
- `GET /api/energy/trend`
- `GET /api/energy/rank`
- `GET /api/anomaly/list`（支持 `page/page_size/sort/severity/status`）
- `GET /api/anomaly/detail`
- `GET /api/anomaly/history`
- `POST /api/anomaly/action`（`ack|ignore|resolve`）
- `POST /api/anomaly/note`（结构化复盘记录）
- `GET /api/anomaly/export`（筛选导出 CSV）
- `GET /api/ai/stats`（默认近24小时调用统计）
- `POST /api/ai/evaluate`（模板/LLM 对比评估）
- `POST /api/ai/feedback`（诊断质量人工标记）
- `GET /api/metrics/overview`
- `GET /api/metrics/saving-potential`
- `POST /api/ai/diagnose`（provider: `template|llm|auto`，含 `fallback_used`）
- `POST /api/ai/analyze`（基于当前分析范围生成结构化结论）
- `GET /api/system/health`

## 回归命令

```powershell
python scripts/run_backend_tests.py
python scripts/smoke_test_api.py
python scripts/prepare_standard_kb.py
npm run test:e2e
npm run test:acceptance
npm run test:all
# 可选：真实 DeepSeek 连通性探测（需先设置 OPENAI_API_KEY）
npm run test:llm-live
```

## LLM 配置（DeepSeek Chat）

默认 `provider=llm/auto` 走 OpenAI-compatible 协议，默认目标为 DeepSeek：

```powershell
$env:OPENAI_BASE_URL="https://api.deepseek.com"
$env:OPENAI_MODEL="deepseek-chat"
$env:OPENAI_API_KEY="你的Key"
$env:OPENAI_TIMEOUT_SEC="20"
$env:OPENAI_MAX_RETRIES="2"
$env:RAGFLOW_STANDARD_DATASET_IDS="标准规范库dataset id，多个用逗号分隔"
```

`POST /api/ai/diagnose` / `POST /api/ai/analyze` 的 `provider` 参数行为：
- `template`：只走模板兜底
- `llm`：直连 DeepSeek，失败直接返回真实错误
- `auto`：优先 DeepSeek，当前默认也不再自动切本地模板；失败直接返回真实错误

前端调用策略：
- 页面刷新、切换建筑、切换时间范围时，不会自动请求在线 LLM
- 只有点击“AI 生成分析结论”或“生成诊断”时，才会触发在线分析
- 页面会明确显示本次结果来源或真实失败原因

运行态会写入 `data/runtime/ai_calls.jsonl`，用于统计降级率与平均耗时；日志不落盘 API Key。
`scripts/run_regression.py` 会输出 `data/runtime/regression_summary.json`，供系统健康接口读取。

## 闭环流程

1. 在“能耗分析”页切换建筑、时间范围、分析类型，查看趋势、结构、天气联动与同类对比
2. 点击“AI 生成分析结论”，进入智能助手查看结构化分析建议
3. 在故障监控页筛选并定位异常
4. 点击“确认/忽略/完成”提交处理动作
5. 在异常详情查看处理时间线、填写复盘记录并导出 CSV

## 交付检查清单

1. `python backend/server.py` 启动成功，首页可见系统与回归状态标签。
2. `npm run test:all` 全绿；`npm run test:acceptance` 全绿。
3. 如配置 Key，`npm run test:llm-live` 返回 `status=pass`。
4. 异常详情可保存复盘记录，且 `/api/anomaly/export` 可下载 CSV。
5. 切换 `provider=llm/auto` 失败时会返回真实错误，不再伪装为模板成功。

