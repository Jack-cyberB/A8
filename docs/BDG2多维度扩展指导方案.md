# BDG2 多维度扩展指导方案

## 1. 目的

当前 A8 已完成电力分析主链路。下一步不建议继续使用“水 / 空调 / 环境”这类泛化表述，而应直接对齐 BDG2 原始可用表计和比赛叙事，形成更真实的扩展路线。

本方案用于明确：

- 除电力外，后续优先接入哪些维度
- 为什么这样排序
- 当前 7 栋代表建筑能支撑哪些维度
- 前端分析类型应该如何命名

## 2. 数据依据

### 2.1 官方数据集说明

根据 BDG2 官方仓库和赛题资料，BDG2 不仅包含电力，还包含多种建筑级小时数据：

- `electricity`
- `chilledwater`
- `hotwater`
- `steam`
- `water`
- `irrigation`
- `gas`
- `solar`

同时配套：

- `metadata` 建筑基础信息
- `weather` 天气环境数据

### 2.2 本地 metadata 统计

基于本地 [metadata.csv](/D:/Project/2026/A8/data/raw/bdg2/data/metadata/metadata.csv) 统计，表计覆盖情况如下：

| 维度 | 覆盖建筑数 |
| --- | ---: |
| electricity | 1578 |
| chilledwater | 555 |
| steam | 370 |
| hotwater | 185 |
| gas | 177 |
| water | 146 |
| irrigation | 37 |
| solar | 5 |

结论：

- `electricity` 仍然是当前最稳的核心维度
- `chilledwater` 是电力之后最值得优先扩展的能源介质
- `water` 和 `gas` 也有明确业务价值
- `hotwater / steam` 可作为后续补充
- `irrigation / solar` 当前不适合作为比赛主线

## 3. 当前 7 栋代表建筑支撑情况

固定展示建筑：

- `Panther_education_Genevieve（教学楼）`
- `Panther_education_Jerome（实验楼）`
- `Panther_office_Patti（办公楼）`
- `Panther_lodging_Marisol（宿舍）`
- `Panther_assembly_Denice（体育馆）`
- `Fox_public_Martin（图书馆）`
- `Fox_food_Scott（食堂）`

对应表计覆盖：

| 建筑 | electricity | chilledwater | water | gas | hotwater |
| --- | --- | --- | --- | --- | --- |
| Panther_education_Genevieve | Yes | - | Yes | - | - |
| Panther_education_Jerome | Yes | Yes | Yes | Yes | - |
| Panther_office_Patti | Yes | - | Yes | Yes | - |
| Panther_lodging_Marisol | Yes | Yes | - | - | - |
| Panther_assembly_Denice | Yes | - | Yes | - | - |
| Fox_public_Martin | Yes | Yes | - | - | Yes |
| Fox_food_Scott | Yes | Yes | - | - | - |

结论：

- `chilledwater` 在当前代表建筑里覆盖最好，最适合作为第二主维度
- `water` 也有足够代表性，适合第三步落地
- `gas` 在展示集内覆盖较弱，但仍可作为专项扩展维度
- `hotwater` 在当前展示集中只有 1 栋，不适合马上做成主路径

## 4. 推荐扩展顺序

### 第一阶段

- `electricity`
- `chilledwater`
- `weather`

目标：

- 从“只会看电”升级到“能解释冷站和空调侧负荷变化”
- 支撑夏季高负荷、冷量异常、冷冻水侧节能分析

### 第二阶段

- `water`

目标：

- 支撑夜间基线偏高、跑冒滴漏、异常用水、食堂/宿舍/教学楼用水对比

### 第三阶段

- `gas`

目标：

- 支撑食堂、生活热水、锅炉或冬季供能类场景

### 第四阶段

- `hotwater`
- `steam`

目标：

- 用于冬季供热或特定建筑热媒分析
- 只在展示建筑和样本池都准备充分后再纳入主界面

### 当前不建议纳入主线

- `irrigation`
- `solar`

原因：

- 覆盖率太低
- 比赛答辩价值不高
- 容易分散主线

## 5. 前端分析类型命名建议

分析类型应从泛化表述改为真实介质表述：

- `电力`
- `冷冻水`
- `用水`
- `燃气`

不建议继续使用：

- `空调`
- `环境`

原因：

- `空调` 是系统层概念，不是 BDG2 的原始 meter 维度
- `环境` 更适合作为天气/外部影响因子，不应和能耗介质并列

## 6. 需要单独补数据源的维度

以下内容不能简单说成“BDG2 直接支持”，应单独建设数据源：

- 空调出水温度
- 空调回水温度
- 室内温湿度
- 设备运行状态
- 照明分项
- 新风、送风、阀门状态

这些更适合归入“设备层 / 子系统层”扩展，而不是 BDG2 整栋楼级表计扩展。

## 7. 对当前项目的落地建议

### 产品口径

当前对外可统一表述为：

- 已完成：`电力`
- 下一步优先接入：`冷冻水`
- 随后扩展：`用水`、`燃气`

### 数据建设口径

当前本地清洗目录中已正式落地：

- [electricity_cleaned.csv](/D:/Project/2026/A8/data/raw/bdg2/data/meters/cleaned/electricity_cleaned.csv)
- [weather.csv](/D:/Project/2026/A8/data/raw/bdg2/data/weather/weather.csv)

后续如继续扩展，需要补齐：

1. `chilledwater` 原始表计文件
2. `water` 原始表计文件
3. `gas` 原始表计文件
4. 对应的清洗、标准化和展示集映射流程

### 界面口径

前端分析类型应先改名为真实扩展路线，即使当前只有电力已完全接入，也要避免继续使用误导性分类。

## 8. 最终结论

对 A8 当前阶段，除电力外的推荐优先级固定为：

1. `chilledwater`
2. `water`
3. `gas`
4. `hotwater / steam`

界面分析类型默认采用：

- `电力`
- `冷冻水`
- `用水`
- `燃气`

这套命名既符合 BDG2，也更适合比赛答辩、后续研发和系统正式化叙述。
