# Layout Recipes

Use these recipes when a dashboard needs stronger composition without becoming a card mosaic.

## Recipe 1: Energy Overview

Use for homepage, campus overview, or executive demo pages.

- Top bar: page title, selected scope, freshness timestamp, primary action.
- KPI band: total energy, change vs last period, anomaly count, estimated saving or carbon metric.
- Main row: wide time-series trend on the left, alert or task rail on the right.
- Secondary row: energy type comparison, building ranking, or weather and occupancy context.
- Bottom row: recent abnormal events and AI interpretation summary.

## Recipe 2: Building Analysis

Use for a single building or subsystem deep dive.

- Sticky filter bar with building, parameter, time range, granularity, and reset.
- Hero chart with one dominant trend and optional benchmark.
- Comparison row: same period comparison, weekday vs weekend, or peer building ranking.
- Explanation strip: one short interpretation sentence and one recommended next step.
- Evidence table: hourly or daily records with export support.

## Recipe 3: Alarm And Diagnosis

Use for anomaly pages and O&M response flows.

- Context header: anomaly type, current severity, affected building or device, trigger time.
- Main row: abnormal segment chart on the left, diagnosis or draft handling panel on the right.
- Support row: probable causes, operating steps, recent handling timeline.
- Footer row: related events, evidence data, and feedback outcome.

## Recipe 4: Replay And Acceptance

Use for review, competition replay, or acceptance demos.

- Summary strip: event count, handled rate, AI adoption rate, key energy-saving result.
- Timeline or event table as the main surface.
- Right rail for selected item details, notes, exported evidence, and user feedback.

## Visual Rules

- Keep one dominant chart per row.
- Reserve the strongest accent color for the most important interactive or status signal.
- Put units next to the number, not buried in subtitles.
- Keep titles factual and scoped: "Dormitory Zone A Daily Electricity" is better than "Energy Overview".
- Do not place more than five KPIs in the first band unless two are visibly grouped as secondary.
- Prefer one dense page with strong grouping over many weak tabs.
- On wide screens, keep the chart reading direction left to right and the action panel on the right.
- On laptop widths, stack secondary panels earlier than the main chart only if action urgency is higher than trend reading.
