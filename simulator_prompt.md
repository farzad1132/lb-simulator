# Simulator Prompt

> **This file is read-only.** It is the shared skeleton for every simulator in this repo — do not edit it in place. To start a new simulator, copy it into the new simulator's directory and fill in the `[USER: ...]` placeholders there. All other sections are fixed conventions the agent must follow as-is.

Use this file as the **skeleton** for every new simulator in this repo.
Sections marked `[USER: ...]` must be filled in by you. All other sections are fixed conventions the agent must follow.

## Installation and Documentation

- **Prefer not installing** language runtimes, compilers, or heavy SDKs on the developer's machine: no `brew install` / `apt install` of `node`, `npm`, `cmake`, vcpkg, etc. for the project workflow. Rust applications, you can build it outside of docker and ship it with docker.
- **Use Docker images** for everything repeatable: C++ build, `npm ci` / Vite, and anything else a `Makefile` target needs.
- The **`README.md` in the simulator's directory** is the place describes the overall intent of the simulator, its features, and how it can be used.
- Detailed information should be documented in **docs/** directory.

---

## 1. Simulator Description

We want to simulate performance of load balancing algorithms and approaches using a discrete time simulator.

```
[USER: Describe what this simulator models.
  - What system or algorithm is being simulated?
  - What are the input parameters (ranges, units)?
  - What are the outputs (values, time-series, distributions)?
  - Are there discrete steps or continuous time evolution?
  - Any specific C++ libraries required (e.g. Eigen, Boost, CGAL)?
]
```

---

## 2. REST API Contract

The HTTP API (JSON request/response bodies) is the contract between frontend and backend. Document it here; the agent implements handlers and `fetch` calls to match.

**Convention (fixed):** unary runs use **`POST /api/run`** unless the domain needs multiple resources. Validation errors → **`400`** with body `{"error":"..."}`. Prefer **snake_case** field names in JSON request/response bodies, and keep the TypeScript interfaces in `api.ts` aligned with this section.

```
[USER: Define the request and response JSON shapes for this simulator.
  - What are the input fields (names, types, units, valid ranges)?
  - Are there multiple input "modes" (e.g. different parameter sets selected
    by a mode/type field)? If so, document each mode's required fields.
  - What does the response contain (raw samples, time series, summary
    stats, derived series)?
  - Include 2-3 example request/response JSON pairs covering the main
    modes or edge cases.
  - Is a streaming/incremental update mode needed (SSE or chunked JSON
    lines)? If so, document the route and event format here or in the README.
]
```

For **streaming** updates (optional, later): prefer **SSE** (`GET` + `text/event-stream`) or chunked JSON lines; document the route and event format in the README.

---

## 3. Rust Backend

### 3.1 What the agent must build

- `backend/CMakeLists.txt` — builds the HTTP server; dependencies resolved **inside Docker** (e.g. distro packages in the backend image), not as a hard requirement on the host.
- `backend/src/main.cc` — HTTP server entry (e.g. cpp-httplib): bind `0.0.0.0`, port from `PORT` env or default **8080**, register `POST /api/run`.
- `backend/src/<service>_impl.h/.cc` — **optional** if routing stays tiny; otherwise implement JSON parse/serialize inline in `main.cc` or one small `http_handlers.cc`.
- `backend/src/<algorithm>.h/.cc` — pure simulation/algorithm code (**no** HTTP includes; reusable in another C++ binary).

### 3.2 Server setup (fixed convention)

The C++ process serves **HTTP/1.1 JSON** on port **8080** (or `PORT`). The **browser** never needs to call the backend origin directly in development: **Vite `server.proxy`** maps **`/api`** → `http://backend:8080` so the page stays same-origin and CORS stays simple.

```cpp
// main.cc skeleton (agent fills in real types and routes)
#include <httplib.h>

int main() {
    httplib::Server svr;
    svr.Post("/api/run", [](const httplib::Request &req, httplib::Response &res) {
        // parse JSON body, run algorithm, set res.body + Content-Type
    });
    svr.listen("0.0.0.0", 8080);
    return 0;
}
```

Vendor **cpp-httplib** as a single header under `backend/third_party/httplib.h` unless you standardize on another small library. Use **nlohmann/json** (distro `nlohmann-json3-dev` or equivalent) for parsing and serialization.

### 3.3 Algorithm requirements

```
[USER: describe the algorithm(s) the C++ code must implement.
  - Mathematical definition or pseudocode
  - External C++ libraries needed
  - Performance constraints (single-threaded vs parallel, target latency)
  - Any numerical methods, data structures, or I/O formats
]
```

---

## 4. Frontend (React + TypeScript + Vite)

### 4.1 What the agent must build

- `frontend/` — Vite + React + TypeScript project
- `frontend/src/App.tsx` — top-level layout: controls panel + visualization area
- `frontend/src/components/Controls.tsx` — all parameter inputs (sliders, number fields, dropdowns)
- `frontend/src/components/Visualization.tsx` — chart(s) / canvas displaying simulation output
- `frontend/src/hooks/useSimulator.ts` — **`fetch`** to `POST /api/run`; exposes `run()`, `reset`, state
- `frontend/src/api.ts` — TypeScript types matching the JSON contract (hand-written; keep in sync with §2)

### 4.2 UI requirements (fixed conventions)

- **Controls panel** (left or top): one input per simulation parameter; sliders for continuous ranges, number inputs for discrete values. Show current value next to each slider.
- **Visualization area** (main area): chart or canvas chosen to best fit the output type:
  - Time-series / line data → Recharts `<LineChart>`
  - Distributions / histograms → Recharts `<BarChart>`
  - 2-D spatial / custom → D3 or `<canvas>`
  - Scientific multi-axis → Plotly
- **Run / Stop / Reset** buttons always visible.
- **Status bar**: shows `idle | running | error` + elapsed time.
- **No page reload needed** to change parameters and re-run.
- Dark background preferred (easier to read charts).

### 4.3 Layout requirements

```
[USER: describe any specific layout preferences.
  - Single view vs. tabbed views?
  - Multiple charts side by side?
  - Any additional panels (logs, raw data table)?
  - Any specific color scheme?
  - Default parameter values to pre-populate?
]
```

---

## 5. Build & Run

The agent must produce a **README.md** in the simulator directory that explains **how to build and run** (see the opening note at the top of this file). The agent must also provide automation so a developer using **only Docker** can work end-to-end.

**Typical files:**

```
docker-compose.yml      # backend + ui — exact ports in README
Makefile                # targets: up/dev, down, build (all Docker-based)
backend/third_party/    # optional: e.g. httplib.h
```

### README.md (required)

- **Prerequisites** (e.g. Docker with Compose only).
- **How to start the stack** (e.g. `make up` or `docker compose up --build`) and **URLs/ports** (UI, REST API on host if exposed, `/api` proxy from Vite).
- **Parameters** the UI or API expose, at a high level.

### `make dev` / `make up`

Start the services defined in `docker-compose.yml` (Vite with HMR, backend container rebuild on image build, etc.). Prefer **one or two commands** from the README; implementation may use `docker compose up` with bind mounts for live reload. **Do not** require a host `npm install` for the default workflow; `npm ci` should run **inside** the UI image or a dedicated build stage.

### Docker Compose topology (typical)

```
browser → ui (Vite, :3000) ──(same origin, /api)──► backend (HTTP JSON, :8080)
```

- Configure **`server.proxy` in `vite.config.ts`**: `/api` → `http://backend:8080` (or the service name Compose assigns).
- Optional env **`VITE_API_BASE`**: override API prefix (default: same-origin relative `/api`).

---

## 6. Project Structure

The agent must produce this layout (adapt names to the simulator domain):

```
<simulator-name>/
├── backend/
│   ├── CMakeLists.txt
│   ├── Dockerfile
│   ├── third_party/            # optional (e.g. httplib.h)
│   └── src/
│       ├── main.cc
│       ├── <algorithm>.h / .cc   # pure logic, no HTTP
│       └── (optional handlers / small routing helpers)
├── frontend/
│   ├── index.html
│   ├── vite.config.ts
│   ├── tsconfig.json
│   ├── package.json
│   └── src/
│       ├── App.tsx
│       ├── api.ts
│       ├── components/
│       │   ├── Controls.tsx
│       │   └── Visualization.tsx
│       └── hooks/
│           └── useSimulator.ts
├── docker-compose.yml
├── Makefile
└── README.md
```

---

## 7. Coding Standards (mandatory — agent must not deviate)

These are copied from `AGENTS.md` and apply to every simulator:

- **Minimal abstractions.** Fewer layers, fewer files. Optimize for readability over extensibility.
- **No verbose comments.** Comments explain *why*, not *what*. One line max.
- **Reuse across simulators.** Before adding a utility, check if a sibling simulator already has it.
- **Two-phase workflow.** Agent must present a plan and get confirmation before writing any code.
- **Update README.** After building, update `<simulator-name>/README.md` with: what it simulates, how to run it (Docker-based commands, ports, prerequisites), and what parameters are exposed. This is the **canonical** "how do I run this?" for the project; `SIMULATOR_PROMPT.md` is not a substitute.
- **Algorithm code is transport-free.** The `.h/.cc` files containing the core math/algorithm must have **no** HTTP server includes so they can be copied into another C++ project.
- **Ask, don't guess.** If any requirement in sections 1–4 is ambiguous, ask in the planning phase. Never leave a `TODO` in generated code.

---

## 8. Agent Checklist

Before presenting the plan, the agent must confirm:

- [ ] Read this file (`SIMULATOR_PROMPT.md`) in full
- [ ] Read `AGENTS.md` in full
- [ ] All `[USER: ...]` placeholders in sections 1–4 are filled in by the user
- [ ] REST request/response shape in §2 is agreed upon
- [ ] Visualization type is decided (Recharts / D3 / Plotly / canvas)
- [ ] No ambiguous requirements remain — all questions asked and answered

---

## 9. Lessons learned (from simulators built in this repo)

These are **not** hard requirements, but they avoid repeated pain:

- **Vite proxy:** point **`/api`** at the backend service so the browser stays on one origin; avoids CORS and mixed-content surprises in development.
- **Stable JSON:** prefer **snake_case** field names in JSON to match common REST style; keep TypeScript interfaces in `api.ts` aligned with §2.
- **Header-only HTTP:** vendoring **cpp-httplib** keeps the Docker build simple compared to pulling in a full application framework.
- **Single source of truth for commands:** the simulator **`README.md`** should list the exact `docker` / `make` / `docker compose` commands; avoid duplicating long procedures only in the prompt file.