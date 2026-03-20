# A8 Monorepo (V0 Deliverable)

A8 建筑能源智能管理系统单仓库版本：
- 前端：Vue3 + Element Plus + ECharts
- 后端：Python 轻量 API + 规则异常检测
- 数据：BDG2 电力时序 + 司空知识条目

## 目录

- `frontend/` 页面与交互
- `backend/` 接口与规则逻辑
- `data/raw/` 原始数据
- `data/normalized/` 标准化数据与质量报告
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
- `GET /api/anomaly/list`（支持 `page/page_size/sort/severity`）
- `GET /api/anomaly/detail`
- `GET /api/metrics/overview`
- `GET /api/metrics/saving-potential`
- `POST /api/ai/diagnose`

## 回归命令

```powershell
python -m unittest backend.tests.test_repository -v
python scripts/smoke_test_api.py
npm run test:e2e
npm run test:all
```
