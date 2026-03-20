# A8 Monorepo (V1 Closed Loop)

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
- `GET /api/energy/trend`
- `GET /api/energy/rank`
- `GET /api/anomaly/list`（支持 `page/page_size/sort/severity/status`）
- `GET /api/anomaly/detail`
- `GET /api/anomaly/history`
- `POST /api/anomaly/action`（`ack|ignore|resolve`）
- `GET /api/ai/stats`（默认近24小时调用统计）
- `GET /api/metrics/overview`
- `GET /api/metrics/saving-potential`
- `POST /api/ai/diagnose`（provider: `template|llm|auto`，含 `fallback_used`）

## 回归命令

```powershell
python -m unittest backend.tests.test_repository -v
python scripts/smoke_test_api.py
npm run test:e2e
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

## 闭环流程

1. 在故障监控页筛选并定位异常
2. 点击“确认/忽略/完成”提交处理动作
3. 在异常详情查看处理时间线
4. 在智能助手生成诊断并写入备注草稿
