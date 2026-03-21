---
name: polish-dashboard-states
description: Polish loading, empty, error, filter, table, and tab states for data-heavy dashboards and product UIs. Use when an analytics page feels unfinished, jumpy, or confusing during async loading, no-data cases, filtering, or dense record browsing.
---

# Polish Dashboard States

Use this skill when the page structure is mostly present but the product experience still feels rough. The goal is to make data-heavy screens feel stable, trustworthy, and demo-ready even before every dataset is available.

## Quick Start

1. List each async region on the page.
2. Define loading, empty, error, and recovered states for each region.
3. Make filters explain the current scope and offer a clear reset path.
4. Tune table density so values remain readable on laptop screens.
5. Preserve context when switching tabs, pages, or time ranges.

## Rules

- Every async block needs an explicit state contract. Blank whitespace is not a valid state.
- Loading placeholders should resemble final layout so the page feels stable while fetching.
- Empty states should explain why there is no data and what the user can do next.
- Error states should separate retryable fetch errors from permission, configuration, or unsupported-scope problems.
- Filter bars should always show current scope, not just input widgets.
- Dense tables must display units, freshness, and row priority without turning into spreadsheet noise.
- If a tab switch resets important context, the page will feel fragile in demos.
- Status text should sound operational and concrete, not decorative.

## Recommended Order

1. Filter and scope bar.
2. Loading and skeleton states.
3. Empty and error states.
4. Dense table and list readability.
5. Tab, drawer, and selection persistence.

## Good Defaults For A8

- Keep a visible "data time range" or "last updated" hint near filters.
- Use operator copy such as "No abnormal records in the selected period" instead of generic "No Data".
- Keep retry actions close to failed panels.
- Preserve selected building or alarm context when reopening the AI or detail drawer.

## References

- Read `references/state-recipes.md` for state contracts, copy patterns, filter rules, and dense-table guidelines.
