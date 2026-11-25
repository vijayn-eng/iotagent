# 🏭 Production Scheduling & Sequencing Agent — Streaming Agents on Confluent Cloud

A real-time **production scheduling and sequencing agent** for a discrete-manufacturing plant,
built entirely with **Confluent Cloud for Apache Flink Streaming Agents** (`CREATE MODEL`,
`CREATE TOOL`, `CREATE AGENT`, `AI_RUN_AGENT`) and Claude on Amazon Bedrock.

Every time the shop floor changes (a paint robot faults, a coil runs short, a CRITICAL order
lands, a press starts vibrating), the agent **re-sequences that line within seconds**. It
minimises paint and die changeovers, protects due dates, reroutes orders to a sister line, and
**takes action** through tools (MES move, ERP expedite, CMMS work order, andon to the
supervisor). The plant simulator consumes the agent's plan and executes it, so the loop is closed.

```
 Plant simulator (local)                         Confluent Cloud
 ─────────────────────────      ┌──────────────────────────────────────────────────────────────┐
  MES dispatch list   ───────►  │ work_orders ─┐                                                │
  IIoT telemetry      ───────►  │ machine_telemetry ─┼─► Flink: UNION + SESSION window per line │
  WMS line-side stock ───────►  │ material_inventory ┘        │                                 │
                                │                     line_snapshots (one text snapshot / line)│
                                │                             │                                 │
                                │       AI_RUN_AGENT(production_scheduling_agent)               │
                                │        ├─ model: Claude (Bedrock)                             │
                                │        └─ tools: reroute_order · expedite_material ·          │
                                │                  schedule_maintenance · notify_supervisor     │
                                │                             │                                 │
  executes agent's plan ◄────── │                   production_schedule ────────────────────────┼──► Dashboard (local)
 (reroutes, runs seq[0])        └──────────────────────────────────────────────────────────────┘
```

## The plant and the agent's rules

| | |
|---|---|
| **Lines** | `LINE-1`, `LINE-2`, `LINE-3`: serial cells **PRESS → PAINT → ASSY**. Any machine DOWN stops the line. |
| **Sister lines** | 1 → 2, 2 → 3, 3 → 1 (reroute targets) |
| **Paint changeover** | same colour 0 min · lighter → darker 10 min · darker → lighter 30 min (WHITE → SILVER → GRAY → RED → BLUE → BLACK) |
| **Die change** | +15 min when `material_id` changes (steel coil ↔ aluminium sheet) |
| **Priorities** | CRITICAL > RUSH > STANDARD, each with a due time in minutes |

The agent diagnoses the line (`NONE / MACHINE_DOWN / MACHINE_DEGRADED / MATERIAL_SHORTAGE /
RUSH_ORDER / MULTIPLE`), reroutes orders that can't wait out a repair, sequences the rest with
start/end minutes, flags orders that will miss their due time, calls tools, and returns
structured JSON that Flink parses into `production_schedule`.

| Scenario (simulator) | What the agent should do |
|---|---|
| normal | Sequence light → dark, grouped by material. Severity < 20, **no tool calls**. |
| 🔧 breakdown: `Lx-PAINT` DOWN, 90–150 min repair | Reroute the RUSH order that can't wait to the sister line, start the rest after the repair, `schedule_maintenance` (immediate), `notify_supervisor`. Severity ≥ 80. |
| 📦 shortage: coil/sheet short, inbound in 2–4 h | `expedite_material` for the shortfall, push orders that need the short material to the end, notify. Severity 50–79. |
| 🚨 rush: CRITICAL order due in 35–55 min | Run it first even if that breaks the colour run (accepts a 30 min dark → light changeover). |
| 📳 degraded: press vibration 7.5–11 mm/s, OEE < 0.65 | `schedule_maintenance` in the first changeover gap of the new sequence. |

## Repository

| Path | Purpose |
|---|---|
| `sql/` | **All Flink SQL, single source of truth.** Terraform submits these files, and you paste the same files during a live build. |
| `sql/05_agent.sql` | The agent: plant rules, scheduling heuristics, tool policy, output contract |
| `sql/06_line_snapshots.sql` | Fuses 3 streams into one text snapshot per line (event-time SESSION window, 3 s gap) |
| `sql/07_schedule.sql` | `AI_RUN_AGENT` on each snapshot, then JSON parsed into `production_schedule` |
| `terraform/` | Environment, Kafka, Schema Registry, Flink pool, Bedrock connection, JAR artifact, every statement; writes `.env` |
| `tools-udf/` | Java UDF tools (mock MES/ERP/CMMS/andon). `target/scheduling-tools.jar` is pre-built. |
| `producer/plant_simulator.py` | Plant simulator. Publishes bursts per line, **consumes the agent's plan and executes it**. |
| `dashboard/app.py` | Streamlit control room: line status, live Gantt of the agent's schedule, decision log, disruption buttons |

## Prerequisites

- Confluent Cloud account + **Cloud resource management** API key/secret
- AWS IAM user (long-lived key) with `bedrock:InvokeModel`, and **model access granted** in the
  Bedrock console for Claude Sonnet 4.5 in `us-east-1`
- Terraform ≥ 1.0, Python ≥ 3.10

The region and model are set in `terraform/main.tf` → `locals` (`aws_region`, `bedrock_region`,
`bedrock_model_id`). The inference-profile prefix must match the Bedrock region (`us.` for
us-east-1, `apac.` for ap-south-1, and so on). To use a different or newer Claude model, change
`bedrock_model_id` to an ID your account can invoke.

## Deploy & run

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars      # 4 values: CC key/secret, AWS key/secret
terraform init && terraform apply                 # ~5–8 min; writes ../.env
cd ..
python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt

python producer/plant_simulator.py                # terminal 1
streamlit run dashboard/app.py                    # terminal 2 → http://localhost:8501
```

Inject a disruption on demand from the dashboard sidebar, or from a shell:

```bash
python producer/plant_simulator.py --inject LINE-2:breakdown   # breakdown | shortage | rush | degraded | normal
python producer/plant_simulator.py --disruption-rate 0 # only on-demand disruptions (quiet plant)
```

Teardown: `cd terraform && terraform destroy`.

## Demo talk track (~15 min)

1. **The problem (1 min).** Schedules are built nightly in the APS/ERP and are stale by the
   first breakdown. Planners firefight on spreadsheets. We'll make sequencing *event-driven*.
2. **Data in motion (2 min).** Start the simulator. In the Flink workspace:
   `SELECT * FROM machine_telemetry;` / `SELECT * FROM work_orders;`. These are three systems
   (MES, IIoT, WMS) on one platform, governed by Schema Registry.
3. **Context engineering with Flink (3 min).** Open `sql/06_line_snapshots.sql`, then
   `SELECT line_id, open_orders, snapshot_text FROM line_snapshots;`. Flink joins
   the streams into one live, per-line context, with no batch ETL and no vector DB.
4. **The agent (3 min).** Run `SHOW AGENTS; SHOW TOOLS; DESCRIBE AGENT production_scheduling_agent;`.
   Walk through the prompt: plant rules, heuristics, tool policy, JSON contract. The tools are
   governed Flink functions: in production `reroute_order` calls the MES API.
5. **Live disruption (4 min).** In the dashboard, select LINE-2 and click **🔧 Paint robot breakdown**.
   Within ~30 s: LINE-2 turns *Critical*, the RUSH order disappears from LINE-2's Gantt and
   **shows up on LINE-3's** in the next cycle (the simulator executed the agent's reroute), and the
   decision log shows `reroute_order, schedule_maintenance, notify_supervisor`. Then click
   **🚨 Critical rush order** and watch the ★ order jump to position 1.
6. **Lineage & ops (2 min).** Stream Lineage: 3 sources → snapshots → agent → schedule. Every
   decision is an immutable, replayable event: audit trail, eval set, and feed for the MES.

Useful queries during the demo:

```sql
SELECT line_id, disruption, severity, actions_taken, reasoning FROM production_schedule;
SELECT line_id, sequence FROM production_schedule WHERE disruption <> 'NONE';
SELECT raw_response FROM production_schedule;     -- full agent output incl. tool results
```

## Building the agent live (instead of one-command)

```bash
terraform apply -var deploy_flink_pipeline=false   # infra + tables + functions only
```

Then in the Flink workspace (catalog = env, database = cluster; see `terraform output`), paste in order:

1. `sql/03_model.sql`
2. the four `sql/04_tools/*.sql`
3. `sql/05_agent.sql`
4. `SET 'sql.tables.scan.idle-timeout' = '5 s';` + `sql/06_line_snapshots.sql`
5. `sql/07_schedule.sql`

## Troubleshooting

- **No schedules but snapshots are flowing.** Check the `sequence-production-*` statement.
  Bedrock auth/model-access errors show up there. `handle_exception='continue'` skips a bad
  agent run. More than 5 consecutive failures stops the statement.
- **Snapshots stop after a few minutes.** The idle-timeout fix (`5 s` on the snapshots statement)
  is missing. Confluent Cloud's progressive idleness otherwise stalls the session windows.
- **Changing a statement's SQL** requires `terraform apply -replace="module.schedule[0].confluent_flink_statement.this"`
  (or `module.snapshots[0]`, `module.agent[0]`). Replaced INSERT/CTAS statements reprocess from earliest.
- **Simulator stdout looks quiet in the background.** It's buffered. Check the topics instead.
- Rebuild the tools JAR (only if you change the Java): `cd tools-udf && mvn -q package` (JDK 11+) or `./build.sh` (Docker).

## Taking it toward production

- Replace the mock tool bodies with real MES/ERP/CMMS calls, or expose those systems through an MCP server
  and register it as an MCP tool for the agent (see the Streaming Agents docs).
- Feed real sources through managed connectors (OPC-UA/MQTT telemetry, SAP/ERP CDC, MES events).
- Add a deterministic guardrail statement that rejects plans where an order id is missing or
  duplicated before the plan reaches the MES.
- Keep `production_schedule` as the evaluation set: replay it against prompt or model changes.
