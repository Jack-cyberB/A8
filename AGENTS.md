# A8 Project Agent Guide

## 1. Startup Rule

Before planning or coding, always treat the following four documents as the source context for this repository:

- `D:/Project/2026/A8/docs/A8.pdf`
- `D:/Project/2026/A8/docs/A8赛题指导手册_V3.0.docx`
- `D:/Project/2026/A8/docs/直播答疑录音文档.docx`
- `D:/Project/2026/A8/docs/A8项目整体计划与技术路线说明.md`

If they conflict, use this priority:

1. `直播答疑录音文档.docx`
2. `A8赛题指导手册_V3.0.docx`
3. `A8.pdf`
4. `A8项目整体计划与技术路线说明.md`

The live Q&A is highest priority because it clarifies what is and is not mandatory in the competition.

## 2. Project Nature

This project is a competition-oriented building energy intelligent management system, not yet a production platform.

The system should be understood as:

`energy dataset + query/statistics + visualization + smart O&M assistant + LLM integration`

Main audience:

- Primary: `to B` users such as O&M staff and project managers
- Secondary: campus or building-side end users such as teachers/students for lightweight query scenarios

Core judging logic:

- Whether the system can support energy management and O&M scenarios
- Whether it demonstrates practical AI/agent capability
- Whether it helps explain energy saving / dual-carbon value
- Whether the system is stable, complete, and easy to present

## 3. What Is Actually Required

The competition does **not** require all enterprise functions. The following are important:

- A self-built or organized energy dataset
- Multi-condition query and statistical analysis
- At least basic visualization
- Domain knowledge + LLM/agent capability
- A complete front-end workflow that can be demonstrated

The competition does **not** strictly require:

- Real physical device integration
- Real-time streaming hardware access
- A full permission system
- An APP
- A fully local model

Allowed and recommended:

- Use imported/historical data
- Use open datasets such as BDG2
- Use online LLM APIs if helpful
- Use MCP internally to connect model and tools/data

## 4. Current Technical Reality

The current repository is a lightweight transitional implementation, created to reach V0/V1/V2 quickly.

Current implemented stack:

- Frontend: `Vue 3 + Element Plus + ECharts`
- Backend: `Python lightweight HTTP server`
- Data layer: `CSV / JSON / JSONL`
- Runtime state: `jsonl` files under `data/runtime/`
- AI: `template diagnosis + DeepSeek API + fallback`
- Testing: `Python tests + API smoke + Playwright E2E`

This is **not** the final target architecture. It is a fast-delivery prototype architecture.

## 5. Long-Term Target Architecture

The user’s original intended architecture is still the preferred long-term direction:

- Frontend: `Vue`
- Main backend: `Spring Boot`
- Database: `MySQL`
- AI service: `Python`
- Knowledge base / RAG: `RAGFlow`

Interpretation rule:

- Current Python backend is a transitional implementation
- Current file-based storage is transitional
- Current AI retrieval is lightweight, not full RAGFlow yet

Future development should avoid breaking the possibility of migrating toward:

`Vue + Spring Boot + MySQL + Python AI + RAGFlow`

## 6. Phase Definition

Use the following definitions consistently:

- `V0`: minimal demo prototype
- `V1`: business closed loop
- `V2`: deliverable and evaluable version

Current repository state should be treated as:

- V2-level prototype / competition deliverable candidate
- Not yet a final polished competition work
- Not yet converged to the long-term formal architecture

## 7. Roadmap Priority

When deciding next development steps, use this order of priority.

### Priority A: Competition completion first

Prefer improving:

- feature completeness
- scenario coverage
- front-end polish
- demo stability
- documentation completeness

Do **not** jump to large architecture migration too early if it hurts competition progress.

### Priority B: Expand analysis scope

The system must not stay limited to electricity only.

Next functional expansion should prefer:

- water
- HVAC-related parameters
- environmental factors
- equipment state

Electricity-only is acceptable as a first cut, but not as the final competition story.

### Priority C: Upgrade AI from simple Q&A to domain assistant

AI should support:

- energy data query assistance
- anomaly interpretation
- O&M process guidance
- evidence-based answers
- domain-grounded reasoning

Avoid turning the system into a generic chat app.

### Priority D: Persistence and engineering

Before major migration, strengthen:

- structured persistence for runtime state
- better schema design
- better modular separation
- stable API contracts

If storage migration happens, prefer:

1. `SQLite` as an intermediate step if needed
2. `MySQL` as the formal direction

### Priority E: Converge to formal architecture

Only after the competition-oriented product is sufficiently complete, gradually move toward:

- modular frontend
- Spring Boot backend
- MySQL persistence
- Python AI service separation
- RAGFlow integration

## 8. What To Emphasize In Future Conversations

At the beginning of each future task, first identify:

1. What phase the project is currently in
2. What has already been implemented
3. What the highest-value next step is
4. Whether that next step improves competition score, completion, or long-term architecture

When proposing next steps, favor:

- direct product improvement
- measurable competition value
- stable demo outcomes
- incremental migration readiness

Avoid:

- introducing unfamiliar tools without a clear benefit
- large refactors that do not immediately improve the work
- over-building admin/permission features unless explicitly needed

## 9. Frontend Guidance

The user currently feels the frontend is rough. Treat this as a real product gap.

Frontend work should prioritize:

- more polished dashboard layout
- better chart readability
- better empty/loading/error states
- more professional visual hierarchy
- stable page switching and chart rendering
- visible support for multiple analysis dimensions

Do not settle for “chart can render” as success.

## 10. Documentation Guidance

The competition expects document quality, not just code.

Whenever appropriate, align work toward later output of:

- requirements analysis
- technical solution
- user manual
- dataset description
- demo and acceptance material

Code changes should make those documents easier to write later.

## 11. Default Working Policy

For this repository, default to the following mindset:

- competition deliverable first
- architecture convergence second
- polish and scenario fitness matter a lot
- AI must serve energy/O&M workflows
- every next step should be explainable in答辩

If uncertain between two tasks, prefer the one that:

- improves visible completion
- strengthens the core scenario
- helps explain energy saving / O&M value
- keeps migration to the long-term stack possible

## 12. Git Delivery Policy

For this repository, after each completed development task:

- run the relevant verification first
- commit the finished work with a clear non-interactive git commit message
- push to `origin`

Interpret "completed" as:

- the requested change is implemented
- the key regression or validation for that change has been run when feasible
- the working tree changes related to that task are ready to share

Do not wait for a separate reminder to commit and push once a task is complete.
