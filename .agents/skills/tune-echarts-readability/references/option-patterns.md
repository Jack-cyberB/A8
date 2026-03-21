# Option Patterns

Use these patterns to pick the simplest chart that answers the question.

## Pattern Selection

- Show trend over time: line chart.
- Compare buildings or devices at one point in time: horizontal bar chart.
- Show composition across periods: stacked bar chart.
- Show hourly and daily concentration: heatmap.
- Show correlation such as temperature vs power: scatter chart.
- Show alarm timeline with context: line chart plus anomaly markers.

## Time-Series Pattern

Use for electricity, water, cooling load, temperature, and baseline comparison.

```js
const option = {
  grid: { left: 48, right: 24, top: 36, bottom: 36, containLabel: true },
  legend: { top: 0 },
  tooltip: {
    trigger: 'axis',
    valueFormatter: (value) => `${value.toFixed(1)} kWh`,
  },
  xAxis: { type: 'category', boundaryGap: false, data: labels },
  yAxis: { type: 'value', name: 'kWh' },
  series: [
    { name: 'Actual', type: 'line', smooth: true, symbol: 'none', data: actual },
    { name: 'Baseline', type: 'line', smooth: true, symbol: 'none', lineStyle: { type: 'dashed' }, data: baseline },
  ],
};
```

## Anomaly Highlight Pattern

Use when the chart must prove the system found abnormal behavior.

```js
{
  name: 'Actual',
  type: 'line',
  data: actual,
  markLine: {
    silent: true,
    symbol: 'none',
    data: [{ yAxis: threshold, label: { formatter: 'Warning threshold' } }],
  },
  markPoint: {
    symbolSize: 10,
    itemStyle: { color: '#D94F4F' },
    data: anomalies.map((item) => ({
      coord: [item.label, item.value],
      name: item.type,
    })),
  },
}
```

## Dual-Axis Guardrails

- Use only when units differ and both series are necessary.
- Match axis color to the corresponding series.
- Keep one series visually dominant and the other subdued.
- Explain both units in the tooltip every time.
- Avoid dual-axis charts for screenshots that judges must parse quickly.

## Label And Tooltip Rules

- Axis labels should show compact time or category text, not full timestamps, unless zoomed in.
- Tooltips should include unit and delta when a baseline exists.
- If values are large, format with `k`, `M`, or localized thousands separators.
- Keep decimals minimal and domain-aware.

## Performance And Density

- Use `dataZoom` for long time ranges, but keep the default window meaningful.
- Use `sampling: 'lttb'` or preprocess data when the chart is too dense.
- Prefer a separate detail chart over six overlapped series in one canvas.
