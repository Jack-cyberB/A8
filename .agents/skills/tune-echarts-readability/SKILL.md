---
name: tune-echarts-readability
description: Refine ECharts line, bar, scatter, heatmap, and mixed analytics charts for clearer units, stronger anomaly emphasis, better legends, and operator-friendly tooltips. Use when Codex edits ECharts options or when charts look cluttered, flat, hard to compare, or weak in data storytelling.
---

# Tune ECharts Readability

Use this skill to convert technically correct charts into charts that operators can scan during a demo. Favor answer-first chart design: each chart should reveal one comparison, one trend, or one abnormal signal clearly.

## Quick Start

1. State the chart question in one sentence.
2. Match the question to a chart pattern in `references/option-patterns.md`.
3. Simplify the series count before adding decoration.
4. Make units, baseline, threshold, and abnormal points explicit.
5. Check laptop readability before adding more controls.

## Core Rules

- Use one `yAxis` by default. Add a second only when units differ and both series are essential to the decision.
- Keep time-series charts to 1-4 visible series whenever possible. Split tabs or toggles before stacking too much data.
- Use threshold cues deliberately: `markLine`, `markArea`, `visualMap`, or highlighted anomaly points.
- Use color consistently across legend, line, point, tooltip label, and related KPI.
- Tooltips should answer the operator's next question: current value, unit, compared baseline, and anomaly status.
- If the chart needs a long legend, the comparison model is probably too dense for one panel.
- Prefer readable axes and fewer ticks over technically complete axis labels.
- Keep chart chrome light. Grid lines, shadows, gradients, and area fills should not overpower the data signal.

## Chart Tuning Order

1. Fix semantic mapping: title, legend, units, and time grain.
2. Fix scale choice: single axis, dual axis, min and max, and percent or absolute display.
3. Fix attention cues: anomaly symbols, benchmark, target line, or warning band.
4. Fix tooltip and axis label formatting.
5. Fix responsiveness, sampling, and `dataZoom` only after the chart reads well.

## Good Defaults For A8

- Energy trend: line chart with comparison period or expected baseline.
- Building ranking: horizontal bar chart sorted descending.
- Energy structure: stacked bar only when contribution is the question.
- Anomaly review: time-series with highlighted abnormal points and threshold band.
- Time-of-day pattern: heatmap when showing concentration by hour and day.

## References

- Read `references/option-patterns.md` for chart selection guidance, ECharts option patterns, and anomaly-highlighting snippets.
