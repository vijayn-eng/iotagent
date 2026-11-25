"""Plant simulator for the production scheduling & sequencing agent demo.

Every planning cycle it publishes, for each of 3 lines, one burst of:
  * machine_telemetry   (PRESS, PAINT, ASSY)
  * material_inventory  (line-side stock vs. what the open orders need)
  * work_orders         (the line's open, unsequenced MES dispatch list)

Confluent Cloud Flink fuses each burst into a line snapshot, the Streaming Agent
sequences it, and writes production_schedule. This simulator CONSUMES that
schedule and closes the loop: orders the agent rerouted move to the sister
line, and each line executes the first order of the agent's sequence.

Disruptions:
  * Random: one line gets a scenario each cycle (see --disruption-rate).
  * On demand: the dashboard sidebar (or `--inject LINE-2:breakdown`) writes
    producer/next_scenario.json, which is picked up at the next cycle.

Usage:
  python producer/plant_simulator.py
  python producer/plant_simulator.py --disruption-rate 0.3 --cycle-seconds 30
  python producer/plant_simulator.py --inject LINE-2:breakdown
"""

import argparse
import json
import os
import random
import sys
import threading
import time
from datetime import datetime

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, "..", ".env"), override=True)
SCENARIO_FILE = os.path.join(HERE, "next_scenario.json")

# Topic names == Flink table names (see sql/01_tables).
T_ORDERS = "work_orders"
T_TELEMETRY = "machine_telemetry"
T_INVENTORY = "material_inventory"
T_SCHEDULE = "production_schedule"

NS = "org.apache.flink.avro.generated.record"


def _schema(topic, fields):
    return json.dumps({"type": "record", "name": f"{topic}_value", "namespace": NS,
                       "fields": [{"name": n, "type": t} for n, t in fields]})


# Field order matches the CREATE TABLE column order (physical columns only).
SCHEMAS = {
    T_ORDERS: _schema(T_ORDERS, [
        ("order_id", "string"), ("line_id", "string"), ("product_sku", "string"), ("color", "string"),
        ("quantity", "int"), ("run_minutes", "int"), ("priority", "string"), ("due_in_min", "int"),
        ("material_id", "string"), ("customer", "string"), ("timestamp", "long")]),
    T_TELEMETRY: _schema(T_TELEMETRY, [
        ("machine_id", "string"), ("line_id", "string"), ("station", "string"), ("status", "string"),
        ("oee", "double"), ("temperature_c", "double"), ("vibration_mm_s", "double"),
        ("fault_code", "string"), ("est_repair_min", "int"), ("timestamp", "long")]),
    T_INVENTORY: _schema(T_INVENTORY, [
        ("material_id", "string"), ("line_id", "string"), ("description", "string"),
        ("on_hand_qty", "int"), ("required_qty", "int"), ("inbound_eta_min", "int"), ("timestamp", "long")]),
}

# ----------------------------- Plant model ----------------------------------
LINES = ["LINE-1", "LINE-2", "LINE-3"]
SISTER = {"LINE-1": "LINE-2", "LINE-2": "LINE-3", "LINE-3": "LINE-1"}
STATIONS = ["PRESS", "PAINT", "ASSY"]
COLORS = ["WHITE", "SILVER", "GRAY", "RED", "BLUE", "BLACK"]
MATERIALS = {
    "STL-COIL-12": "1.2mm cold-rolled steel coil",
    "ALU-SHT-08": "0.8mm aluminium sheet",
}
# sku -> (material, minutes per unit)
SKUS = {
    "BRK-220": ("STL-COIL-12", 0.08),   # seat bracket
    "BRK-310": ("STL-COIL-12", 0.10),   # door hinge bracket
    "HSG-140": ("ALU-SHT-08", 0.15),    # battery housing cover
    "HSG-260": ("ALU-SHT-08", 0.18),    # inverter housing
}
CUSTOMERS = ["OEM-NORTH", "OEM-EAST", "EV-STARTUP", "AFTERMARKET"]
PLANT_MINUTES_PER_CYCLE = 15   # each cycle advances the plant clock by 15 minutes
SCENARIOS = ["breakdown", "shortage", "rush", "degraded"]

_order_seq = 10000
_lock = threading.Lock()
latest_plan = {}  # line_id -> {"sequence": [order ids], "rerouted": [order ids]}


def now_ms():
    return int(time.time() * 1000)


def new_order(line_id, priority=None, due=None, color=None, sku=None):
    global _order_seq
    _order_seq += 1
    sku = sku or random.choice(list(SKUS))
    material, rate = SKUS[sku]
    qty = random.choice([200, 300, 400, 500, 600, 800])
    return {
        "order_id": f"WO-{_order_seq}",
        "line_id": line_id,
        "product_sku": sku,
        "color": color or random.choice(COLORS),
        "quantity": qty,
        "run_minutes": max(10, int(round(qty * rate))),
        "priority": priority or random.choices(["STANDARD", "RUSH"], weights=[80, 20])[0],
        "due_in_min": due if due is not None else random.randint(180, 600),
        "material_id": material,
        "customer": random.choice(CUSTOMERS),
    }


backlog = {line: [new_order(line) for _ in range(random.randint(4, 6))] for line in LINES}


# --------------------------- Closing the loop -------------------------------
def apply_agent_plans():
    """Apply the agent's latest decisions: move rerouted orders, run sequence[0]."""
    with _lock:
        plans = dict(latest_plan)
        latest_plan.clear()
    for line, plan in plans.items():
        for oid in plan.get("rerouted", []):
            for o in list(backlog.get(line, [])):
                if o["order_id"] == oid:
                    backlog[line].remove(o)
                    target = SISTER[line]
                    o["line_id"] = target
                    backlog[target].append(o)
                    print(f"  [loop] {oid} rerouted {line} -> {target} (agent decision)")
        seq = plan.get("sequence", [])
        if seq:
            first = next((o for o in backlog[line] if o["order_id"] == seq[0]), None)
            if first:
                backlog[line].remove(first)
                print(f"  [loop] {line} ran {first['order_id']} first (agent sequence)")


def advance_plant():
    for line in LINES:
        # Without an agent plan, the line still completes something now and then.
        if random.random() < 0.3 and len(backlog[line]) > 3:
            backlog[line].pop(random.randrange(len(backlog[line])))
        for o in list(backlog[line]):
            o["due_in_min"] -= PLANT_MINUTES_PER_CYCLE
            if o["due_in_min"] <= 10:          # shipped (possibly late)
                backlog[line].remove(o)
        while len(backlog[line]) < random.randint(4, 6):
            backlog[line].append(new_order(line))


def schedule_listener(stop):
    """Consume production_schedule and stash each line's latest plan."""
    sr = SchemaRegistryClient({"url": os.environ["SCHEMA_REGISTRY_URL"],
                               "basic.auth.user.info": f'{os.environ["SCHEMA_REGISTRY_API_KEY"]}:{os.environ["SCHEMA_REGISTRY_API_SECRET"]}'})
    deser = AvroDeserializer(sr)
    c = Consumer({**kafka_conf(), "group.id": f"plant-sim-{int(time.time())}", "auto.offset.reset": "latest"})
    c.subscribe([T_SCHEDULE])
    while not stop.is_set():
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        try:
            v = deser(m.value(), SerializationContext(m.topic(), MessageField.VALUE))
            seq = [s.get("order_id") for s in json.loads(v["sequence"] or "[]") if isinstance(s, dict)]
            rer = [r for r in json.loads(v["rerouted_orders"] or "[]") if isinstance(r, str)]
            with _lock:
                latest_plan[v["line_id"]] = {"sequence": seq, "rerouted": rer}
            print(f"  [agent] {v['line_id']} plan: {v['disruption']} sev={v['severity']} seq={seq} rerouted={rer}")
        except Exception as e:  # malformed agent output: ignore, keep simulating
            print(f"  [agent] could not parse plan: {e}")
    c.close()


# ----------------------------- Event builders -------------------------------
def telemetry_for(line, scenario):
    out = []
    n = line[-1]
    for st in STATIONS:
        ev = {"machine_id": f"L{n}-{st}", "line_id": line, "station": st, "status": "RUNNING",
              "oee": round(random.uniform(0.78, 0.92), 2), "temperature_c": round(random.uniform(48, 64), 1),
              "vibration_mm_s": round(random.uniform(1.2, 3.4), 1), "fault_code": "", "est_repair_min": 0}
        if scenario == "breakdown" and st == "PAINT":
            ev.update(status="DOWN", oee=0.0, temperature_c=71.5, vibration_mm_s=0.0,
                      fault_code="E-417 paint robot axis-3 servo overload", est_repair_min=random.choice([90, 120, 150]))
        if scenario == "degraded" and st == "PRESS":
            ev.update(status="DEGRADED", oee=round(random.uniform(0.55, 0.62), 2), temperature_c=round(random.uniform(76, 84), 1),
                      vibration_mm_s=round(random.uniform(7.5, 11.0), 1), fault_code="W-212 main bearing vibration high")
        out.append(ev)
    return out


def inventory_for(line, scenario):
    out = []
    short = random.choice(list(MATERIALS)) if scenario == "shortage" else None
    for mat, desc in MATERIALS.items():
        required = sum(o["quantity"] for o in backlog[line] if o["material_id"] == mat)
        if mat == short:
            on_hand, eta = max(0, required - random.choice([200, 300, 400, 600])), random.choice([120, 180, 240])
        else:
            on_hand, eta = int(required * random.uniform(1.3, 2.0)) + 200, random.randint(30, 90)
        out.append({"material_id": mat, "line_id": line, "description": desc, "on_hand_qty": on_hand,
                    "required_qty": required, "inbound_eta_min": eta})
    return out


def prepare_scenario(line, scenario):
    """Make sure the line's backlog makes the scenario interesting."""
    if scenario == "breakdown":
        # A RUSH order that cannot wait for the repair -> agent should reroute it.
        backlog[line].append(new_order(line, priority="RUSH", due=random.randint(70, 110)))
    elif scenario == "rush":
        backlog[line].append(new_order(line, priority="CRITICAL", due=random.randint(35, 55),
                                       color=random.choice(["BLACK", "BLUE"])))
    elif scenario == "shortage":
        mats = {o["material_id"] for o in backlog[line]}
        if len(mats) < 2:  # make sure both materials are present so resequencing matters
            missing = [s for s, (m, _) in SKUS.items() if m not in mats]
            backlog[line].append(new_order(line, sku=random.choice(missing)))


# ------------------------------- Kafka plumbing -----------------------------
def kafka_conf():
    return {"bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"], "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN", "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"]}


def build_serializers():
    sr = SchemaRegistryClient({"url": os.environ["SCHEMA_REGISTRY_URL"],
                               "basic.auth.user.info": f'{os.environ["SCHEMA_REGISTRY_API_KEY"]}:{os.environ["SCHEMA_REGISTRY_API_SECRET"]}'})
    conf = {"auto.register.schemas": False, "use.latest.version": True}  # Flink owns the schemas
    return {t: AvroSerializer(sr, s, conf=conf) for t, s in SCHEMAS.items()}


_key = StringSerializer("utf_8")


def send(producer, sers, topic, ev):
    ev = {**ev, "timestamp": now_ms()}
    producer.produce(topic=topic,
                     key=_key(ev["line_id"], SerializationContext(topic, MessageField.KEY)),
                     value=sers[topic](ev, SerializationContext(topic, MessageField.VALUE)))
    producer.poll(0)


def take_injected():
    if not os.path.exists(SCENARIO_FILE):
        return None
    try:
        with open(SCENARIO_FILE) as f:
            req = json.load(f)
        os.remove(SCENARIO_FILE)
        if req.get("line") in LINES and req.get("scenario") in SCENARIOS + ["normal"]:
            return req["line"], req["scenario"]
    except Exception as e:
        print(f"  ignoring bad {SCENARIO_FILE}: {e}")
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cycle-seconds", type=float, default=25, help="pause between planning cycles (default 25)")
    ap.add_argument("--disruption-rate", type=float, default=0.6,
                    help="probability a random line gets a random disruption each cycle (default 0.6)")
    ap.add_argument("--inject", help="queue a scenario for the next cycle, e.g. LINE-2:breakdown, then exit")
    args = ap.parse_args()

    if args.inject:
        line, scenario = args.inject.split(":")
        with open(SCENARIO_FILE, "w") as f:
            json.dump({"line": line, "scenario": scenario}, f)
        print(f"Queued {scenario} on {line} for the next cycle of the running simulator.")
        return

    producer = Producer({**kafka_conf(), "client.id": "plant-simulator", "linger.ms": 20})
    sers = build_serializers()
    stop = threading.Event()
    threading.Thread(target=schedule_listener, args=(stop,), daemon=True).start()
    print(f"Connected to {os.environ['BOOTSTRAP_SERVERS']}. Simulating {', '.join(LINES)} (Ctrl+C to stop).\n")

    cycle = 0
    try:
        while True:
            cycle += 1
            if cycle > 1:
                apply_agent_plans()
                advance_plant()

            disrupted = take_injected()
            if disrupted is None and random.random() < args.disruption_rate:
                disrupted = (random.choice(LINES), random.choice(SCENARIOS))
            print(f"--- Cycle {cycle} ({datetime.now():%H:%M:%S})"
                  f" {'disruption: ' + disrupted[1] + ' on ' + disrupted[0] if disrupted else 'all lines normal'} ---")

            for line in LINES:
                scenario = disrupted[1] if disrupted and disrupted[0] == line else "normal"
                prepare_scenario(line, scenario)
                for ev in telemetry_for(line, scenario):
                    send(producer, sers, T_TELEMETRY, ev)
                for ev in inventory_for(line, scenario):
                    send(producer, sers, T_INVENTORY, ev)
                for o in backlog[line]:
                    send(producer, sers, T_ORDERS, o)
                print(f"  {line} [{scenario}] {len(backlog[line])} open orders: "
                      + ", ".join(f"{o['order_id']}({o['priority'][0]},{o['color']})" for o in backlog[line]))
                producer.flush()
                time.sleep(0.5)

            sys.stdout.flush()
            time.sleep(args.cycle_seconds)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        producer.flush()


if __name__ == "__main__":
    main()
