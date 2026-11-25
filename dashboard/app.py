"""Production Scheduling & Sequencing Agent - live control-room dashboard.

Consumes (from Confluent Cloud):
  production_schedule   the agent's released plans (one per line per planning burst)
  machine_telemetry     latest machine state per line
  material_inventory    latest line-side stock
Sidebar buttons queue disruptions for the plant simulator (producer/next_scenario.json).

Run:  streamlit run dashboard/app.py
"""

import json
import os
import threading
import time
from collections import deque
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, "..", ".env"), override=True)
SCENARIO_FILE = os.path.join(HERE, "..", "producer", "next_scenario.json")

LINES = ["LINE-1", "LINE-2", "LINE-3"]
STATIONS = ["PRESS", "PAINT", "ASSY"]  # serial line order
SCENARIOS = {"breakdown": "🔧 Paint robot breakdown", "shortage": "📦 Material shortage",
             "rush": "🚨 Critical rush order", "degraded": "📳 Press vibration (degraded)"}

# Dark-surface tokens (reference palette) + fixed status colours.
SURFACE, TEXT, TEXT_2, MUTED, GRID = "#1a1a19", "#f0efec", "#c3c2b7", "#8a8a85", "#383835"
STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}
# Paint colours are the data itself, so the bars wear them literally (outlined for contrast).
PAINT = {"WHITE": "#f4f4f2", "SILVER": "#b9bec4", "GRAY": "#7b7f85",
         "RED": "#c8323a", "BLUE": "#2a64c8", "BLACK": "#151515"}
PAINT_ORDER = list(PAINT)

st.set_page_config(page_title="Production Scheduling Agent", page_icon="🏭", layout="wide")
st.markdown(f"""
<style>
  [data-testid="stMainBlockContainer"] {{ padding-top: 2rem; }}
  div[data-testid="stMetric"] {{ background:{SURFACE}; border:1px solid {GRID}; border-radius:8px; padding:12px 16px; }}
  div[data-testid="stMetric"] label {{ color:{TEXT_2} !important; text-transform:uppercase; letter-spacing:.6px; font-size:.75rem !important; }}
  .card {{ background:{SURFACE}; border:1px solid {GRID}; border-radius:8px; padding:12px 14px; min-height:210px; }}
  .card h4 {{ margin:0 0 6px 0; color:{TEXT}; }}
  .pill {{ display:inline-block; padding:2px 8px; border-radius:4px; font-size:.75rem; font-weight:600; color:#111; }}
  .muted {{ color:{TEXT_2}; font-size:.82rem; }}
  .mach {{ font-family: ui-monospace, monospace; font-size:.8rem; color:{TEXT}; }}
</style>""", unsafe_allow_html=True)


# ------------------------------ Kafka consumer ------------------------------
def _consumer():
    return Consumer({
        "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"], "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN", "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        "group.id": f"prodsched-dashboard-{int(time.time())}", "auto.offset.reset": "latest",
    })


def _list(v):
    try:
        x = json.loads(v) if isinstance(v, str) else v
        return x if isinstance(x, list) else []
    except Exception:
        return []


def _poll(state):
    sr = SchemaRegistryClient({"url": os.environ["SCHEMA_REGISTRY_URL"],
                               "basic.auth.user.info": f'{os.environ["SCHEMA_REGISTRY_API_KEY"]}:{os.environ["SCHEMA_REGISTRY_API_SECRET"]}'})
    deser = AvroDeserializer(sr)
    c = _consumer()
    c.subscribe(["production_schedule", "machine_telemetry", "material_inventory"])
    while True:
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        try:
            v = deser(m.value(), SerializationContext(m.topic(), MessageField.VALUE))
        except Exception:
            continue
        with state["lock"]:
            if m.topic() == "machine_telemetry":
                state["machines"][v["machine_id"]] = v
            elif m.topic() == "material_inventory":
                state["materials"][(v["line_id"], v["material_id"])] = v
            else:
                v["at"] = datetime.fromtimestamp(m.timestamp()[1] / 1000)
                for k in ("sequence", "rerouted_orders", "orders_at_risk", "actions_taken"):
                    v[k] = _list(v.get(k))
                state["plans"].appendleft(v)
                state["latest"][v["line_id"]] = v


@st.cache_resource
def shared_state():
    state = {"lock": threading.Lock(), "plans": deque(maxlen=300), "latest": {},
             "machines": {}, "materials": {}}
    threading.Thread(target=_poll, args=(state,), daemon=True).start()
    return state


# --------------------------------- Helpers ----------------------------------
def severity_status(sev):
    if sev >= 80:
        return "critical", "⛔ Critical"
    if sev >= 50:
        return "serious", "⚠️ Serious"
    if sev >= 20:
        return "warning", "△ Watch"
    return "good", "✓ Normal"


def machine_badge(m):
    s = m["status"]
    color = {"RUNNING": STATUS["good"], "DEGRADED": STATUS["warning"], "DOWN": STATUS["critical"]}.get(s, MUTED)
    icon = {"RUNNING": "●", "DEGRADED": "▲", "DOWN": "■"}.get(s, "○")
    extra = f"<br><span style='color:{color}'>&nbsp;&nbsp;↳ {m['fault_code']}</span>" if m["fault_code"] else ""
    return (f"<div class='mach'><span style='color:{color}'>{icon}</span> {m['machine_id']:<9} "
            f"{s:<8} OEE {m['oee']:.2f} · {m['vibration_mm_s']} mm/s{extra}</div>")


# --------------------------------- Sections ---------------------------------
def sidebar():
    st.sidebar.header("🎛️ Inject a disruption")
    st.sidebar.caption("Queued for the next simulator cycle (~25 s). The agent replans that line.")
    line = st.sidebar.selectbox("Line", LINES, index=1)
    for key, label in SCENARIOS.items():
        if st.sidebar.button(label, width="stretch"):
            with open(SCENARIO_FILE, "w") as f:
                json.dump({"line": line, "scenario": key}, f)
            st.sidebar.success(f"Queued {key} on {line}")
    st.sidebar.divider()
    st.sidebar.markdown(
        "**Pipeline (Confluent Cloud)**  \n"
        "`work_orders` · `machine_telemetry` · `material_inventory`  \n"
        "→ Flink SESSION window per line → `line_snapshots`  \n"
        "→ `AI_RUN_AGENT(production_scheduling_agent)` + tools  \n"
        "→ `production_schedule`")


def kpis(plans, latest):
    disruptions = [p for p in plans if p["disruption"] not in ("NONE", "UNKNOWN")]
    rerouted = sum(len(p["rerouted_orders"]) for p in plans)
    actions = sum(len(p["actions_taken"]) for p in plans)
    at_risk_now = sum(len(p["orders_at_risk"]) for p in latest.values())
    changeover_now = sum(p["changeover_min"] for p in latest.values())
    c = st.columns(5)
    c[0].metric("Plans released", len(plans))
    c[1].metric("Disruptions handled", len(disruptions))
    c[2].metric("Orders rerouted", rerouted)
    c[3].metric("Agent tool actions", actions)
    c[4].metric("At-risk orders", at_risk_now, help=f"Total planned changeover now: {changeover_now} min")


def line_cards(latest, machines, materials):
    cols = st.columns(3)
    for col, line in zip(cols, LINES):
        p = latest.get(line)
        ms = sorted((m for m in machines.values() if m["line_id"] == line), key=lambda m: STATIONS.index(m["station"]) if m.get("station") in STATIONS else 9)
        mats = [v for (l, _), v in materials.items() if l == line]
        if p:
            key, label = severity_status(p["severity"])
            head = (f"<span class='pill' style='background:{STATUS[key]}'>{label}</span> "
                    f"<b style='color:{TEXT}'>{p['disruption']}</b> "
                    f"<span class='muted'>sev {p['severity']} · makespan {p['makespan_min']} min · "
                    f"changeover {p['changeover_min']} min · {p['at']:%H:%M:%S}</span>")
            reason = f"<div class='muted' style='margin-top:6px'>🤖 {p['reasoning']}</div>"
            acts = " ".join(f"<code>{a}</code>" for a in p["actions_taken"]) or "<span class='muted'>no actions</span>"
        else:
            head, reason, acts = "<span class='muted'>waiting for first plan…</span>", "", ""
        mat_html = ""
        for v in mats:
            short = v["on_hand_qty"] < v["required_qty"]
            icon = "■ SHORT" if short else "● OK"
            color = STATUS["critical"] if short else STATUS["good"]
            mat_html += (f"<div class='mach'><span style='color:{color}'>{icon}</span> {v['material_id']} "
                         f"{v['on_hand_qty']}/{v['required_qty']}</div>")
        col.markdown(f"<div class='card'><h4>🏭 {line}</h4>{head}<div style='margin-top:8px'>"
                     f"{''.join(machine_badge(m) for m in ms)}{mat_html}</div>{reason}"
                     f"<div style='margin-top:6px'>{acts}</div></div>", unsafe_allow_html=True)


def gantt(latest):
    rows = []
    for line, p in latest.items():
        risk = set(p["orders_at_risk"])
        for s in p["sequence"]:
            try:
                rows.append({"line": line, "order": s["order_id"], "color": str(s.get("color", "GRAY")).upper(),
                             "priority": s.get("priority", ""), "start": int(s["start_min"]), "end": int(s["end_min"]),
                             "due": int(s.get("due_in_min", 0)), "at_risk": s["order_id"] in risk})
            except (KeyError, TypeError, ValueError):
                continue
    if not rows:
        st.info("The Gantt appears once the agent releases its first schedule.")
        return
    df = pd.DataFrame(rows)
    df["label"] = df.apply(lambda r: ("⚠ " if r.at_risk else "") + r.order + (" ★" if r.priority == "CRITICAL" else ""), axis=1)
    df["dur"] = df["end"] - df["start"]

    base = alt.Chart(df).encode(
        y=alt.Y("line:N", sort=LINES, title=None, scale=alt.Scale(paddingInner=0.35, paddingOuter=0.2), axis=alt.Axis(labelColor=TEXT, labelFontSize=13, ticks=False, domain=False)),
    )
    tooltip = [alt.Tooltip("order:N", title="Order"), alt.Tooltip("color:N", title="Paint"),
               alt.Tooltip("priority:N", title="Priority"), alt.Tooltip("start:Q", title="Start (min)"),
               alt.Tooltip("end:Q", title="End (min)"), alt.Tooltip("due:Q", title="Due in (min)"),
               alt.Tooltip("at_risk:N", title="At risk")]
    bars = base.mark_bar(cornerRadius=4).encode(
        x=alt.X("start:Q", title="Minutes from now",
                axis=alt.Axis(labelColor=TEXT_2, titleColor=TEXT_2, gridColor=GRID, domainColor=GRID, tickColor=GRID)),
        x2="end:Q",
        color=alt.Color("color:N", scale=alt.Scale(domain=PAINT_ORDER, range=[PAINT[c] for c in PAINT_ORDER]),
                        legend=alt.Legend(title="Paint colour (light → dark)", orient="top", labelColor=TEXT_2, titleColor=TEXT_2)),
        stroke=alt.condition("datum.at_risk", alt.value(STATUS["critical"]), alt.value(MUTED)),
        strokeWidth=alt.condition("datum.at_risk", alt.value(2.5), alt.value(1)),
        tooltip=tooltip,
    )
    # Label only bars wide enough to hold text; the colour of the text follows the bar's lightness.
    labels = base.transform_filter("datum.dur >= 25").mark_text(align="left", dx=6, fontSize=11).encode(
        x="start:Q", text="label:N",
        color=alt.condition(alt.FieldOneOfPredicate(field="color", oneOf=["WHITE", "SILVER"]),
                            alt.value("#111111"), alt.value("#f5f5f5")),
        tooltip=tooltip,
    )
    chart = (bars + labels).properties(height=alt.Step(56), background=SURFACE,
                                       padding={"left": 10, "right": 10, "top": 10, "bottom": 10}
                                       ).configure_view(stroke=None)
    st.altair_chart(chart, width="stretch")
    st.caption("Each bar is a work order in the agent's sequence. ⚠ red outline = will miss due time · ★ = CRITICAL. "
               "Gaps between bars are paint/die changeovers; a gap at the start is a repair wait.")


def decisions(plans):
    if not plans:
        return
    df = pd.DataFrame([{
        "time": p["at"].strftime("%H:%M:%S"), "line": p["line_id"], "disruption": p["disruption"],
        "severity": p["severity"], "status": severity_status(p["severity"])[1],
        "sequence": " → ".join(s.get("order_id", "?") for s in p["sequence"] if isinstance(s, dict)),
        "rerouted": ", ".join(p["rerouted_orders"]), "at risk": ", ".join(p["orders_at_risk"]),
        "actions": ", ".join(p["actions_taken"]), "reasoning": p["reasoning"],
    } for p in plans])
    only = st.toggle("Only show disruptions", value=False)
    if only:
        df = df[df["disruption"] != "NONE"]
    st.dataframe(df, width="stretch", hide_index=True, height=360,
                 column_config={"severity": st.column_config.ProgressColumn("severity", min_value=0, max_value=100, format="%d"),
                                "reasoning": st.column_config.TextColumn(width="large")})


def main():
    state = shared_state()
    sidebar()
    st.title("🏭 Production Scheduling & Sequencing Agent")
    st.caption("Streaming Agents on Confluent Cloud · Flink + Claude replan every line in seconds when the shop floor changes")
    with state["lock"]:
        plans, latest = list(state["plans"]), dict(state["latest"])
        machines, materials = dict(state["machines"]), dict(state["materials"])
    kpis(plans, latest)
    st.subheader("Line status")
    line_cards(latest, machines, materials)
    st.subheader("Current agent schedule")
    gantt(latest)
    st.subheader("Agent decision log")
    decisions(plans)
    time.sleep(2)
    st.rerun()


main()
