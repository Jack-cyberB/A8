# State Recipes

Use these patterns to make dashboards feel complete during demos and daily use.

## State Contract Per Region

Each async panel should define:

- `loading`: what skeleton or placeholder appears first.
- `empty`: what message appears when the query succeeds but returns no meaningful rows.
- `error`: what message and retry affordance appears when the request fails.
- `stale`: whether prior data remains visible during refresh.
- `updatedAt`: how freshness is shown.

## Loading Patterns

- KPI area: show placeholder numbers with fixed height to avoid layout jump.
- Chart area: keep frame, title, legend shell, and axis skeleton visible.
- Table area: show 5-8 placeholder rows with realistic column widths.
- Drawer or detail panel: keep summary structure visible before content arrives.

## Empty-State Copy

Prefer scope-aware copy:

- "No abnormal records in the selected period."
- "No water data has been imported for this building yet."
- "No comparison result is available because the baseline period is incomplete."

Add one next action when helpful:

- Change time range.
- Reset filters.
- Import data.
- Check data source freshness.

## Error-State Copy

- Query failure: explain that data could not be fetched and offer retry.
- Unsupported scope: explain which filters conflict.
- Missing configuration: explain what is not configured yet.
- Never show raw stack traces in-page.

## Filter Bar Rules

- Show selected scope in plain text near the controls.
- Keep the reset action always visible once more than one filter is active.
- Avoid multi-row filter bars unless the page is analysis-heavy and clearly sectioned.
- Date range and granularity should read together.

## Dense Table Rules

- Put the most decision-relevant column first after the entity name.
- Keep units in column titles or formatted cell suffixes.
- Use row emphasis for severity or exception, not random zebra contrast.
- Right-align numeric columns and keep decimal precision consistent.
- On smaller screens, collapse secondary metadata into one supporting column before horizontal scroll becomes excessive.

## Persistence Rules

- Preserve selected building, time range, and tab when opening details.
- Preserve table filters when returning from a detail drawer.
- Keep chart and table scope synchronized unless there is a clear reason to decouple them.
