---
name: design-energy-dashboard
description: Design and refine B-end building energy or O&M dashboards, workbenches, and analysis pages in Vue, Element Plus, and ECharts. Use when Codex needs to improve dashboard layout, KPI composition, chart placement, visual hierarchy, or demo readiness for data-heavy energy management UIs.
---

# Design Energy Dashboard

Use this skill to turn a rough energy page into a demo-ready product surface for operators, project managers, and judges. Favor business clarity, readable hierarchy, and visible energy-saving or O&M value over decorative UI.

## Quick Start

1. Identify the page type: overview, analysis, alarm, diagnosis, or replay.
2. Identify the primary decision the operator should make within 5 seconds.
3. Pick one layout recipe from `references/layout-recipes.md`.
4. Keep one dominant chart or workspace per viewport row.
5. Make the page answer three questions fast: what is happening, why it matters, what to do next.

## Working Rules

- Design for `to B` scanning first. Title, scope, current status, and next action should be obvious before any deep reading.
- Show business value in the surface itself: energy trend, anomaly count, carbon or saving indicator, or pending O&M action.
- Separate summary, evidence, and action. Do not mix KPIs, charts, and recommendations into one generic card wall.
- Use existing stack patterns when possible. Stay compatible with `Vue 3 + Element Plus + ECharts`.
- Prefer a calm base palette with one primary accent plus warning and danger states. Do not let charts and chrome compete.
- Keep labels operator-friendly: building, time range, parameter, unit, abnormality, compared with last period, suggested action.
- If a page feels empty without charts, the information architecture is weak. Strengthen the page frame, scope controls, and state messaging.

## Page Construction Order

1. Define the data scope block: page title, selected building or campus, time range, freshness, export or refresh actions.
2. Build the summary band: 3-5 KPIs with clear units and one sentence of meaning.
3. Place the dominant chart or work surface.
4. Add secondary evidence panels: comparison, ranking, anomaly list, or device context.
5. Add action support: diagnosis summary, recommended steps, notes, or handoff record.

## What Good Looks Like

- The first screen has a clear focal point.
- The page can be explained in one sentence during a demo.
- The chart area has enough whitespace to read without zooming.
- KPI labels are specific enough to stand alone in a screenshot.
- Empty, loading, and error states are defined instead of falling back to blank panels.

## References

- Read `references/layout-recipes.md` for page structures, grid suggestions, and visual rules for energy and O&M dashboards.
