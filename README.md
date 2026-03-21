# A8 Monorepo (V2 Analysis Workspace)

A8 建筑能源智能管理系统：
- 前端：Vue3 + Element Plus + ECharts
- 后端：Python 轻量 API + 规则异常检测 + 闭环处理状态机
- 数据：BDG2 电力时序 + 司空知识条目

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
```

`POST /api/ai/diagnose` 的 `provider` 参数行为：
- `template`：只走模板诊断
- `llm`：优先走 LLM，失败自动降级模板
- `auto`：与 `llm` 相同，作为默认推荐模式

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
5. 切换 `provider=llm/auto` 失败时仍可降级模板并继续完成处理流程。

