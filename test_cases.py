"""
test_cases.py
-------------
Test cases for the Distributed Food Delivery System.
Run with: python3 test_cases.py

Test Case 1 — Full delivery flow completes successfully
Test Case 2 — Vector clock update rules are correct
Test Case 3 — Concurrent events are detected
Test Case 4 — All 4 processes record state in snapshot
Test Case 5 — Captured global snapshot is consistent
"""

import threading
import sys
from process_base import ProcessBase
from vector_clock import VectorClock
from customer import Customer
from order_processor import OrderProcessor
from restaurant import Restaurant
from delivery_partner import DeliveryPartner
from concurrent_events import analyse
from snapshot_collector import collect, check_consistency

N = 4  # number of processes

# ── Colour helpers ────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"

passed = 0
failed = 0


def run_simulation():
    """Start all 4 processes and wait for them to finish. Returns process list."""
    # Clear registry between test runs
    ProcessBase._registry.clear()

    processes = [
        Customer(0, N),
        OrderProcessor(1, N),
        Restaurant(2, N),
        DeliveryPartner(3, N),
    ]
    threads = [threading.Thread(target=p.run, name=p.name, daemon=True) for p in processes]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    return processes


def check(name: str, condition: bool, expected: str, actual: str):
    """Print pass/fail for one assertion."""
    global passed, failed
    if condition:
        passed += 1
        print(f"  {GREEN}PASS{RESET}  {name}")
    else:
        failed += 1
        print(f"  {RED}FAIL{RESET}  {name}")
        print(f"         Expected : {expected}")
        print(f"         Got      : {actual}")


# ─────────────────────────────────────────────────────────────────
# TEST CASE 1 — Full delivery flow
# ─────────────────────────────────────────────────────────────────
print(f"\n{YELLOW}Test Case 1 — Full delivery flow completes successfully{RESET}")

processes = run_simulation()
customer   = processes[0]
op         = processes[1]
restaurant = processes[2]
delivery   = processes[3]

# 1a — all 26 events logged
all_events = [e for p in processes for e in p.event_log]
check("26 events logged",
      len(all_events) == 26,
      "26", str(len(all_events)))

# 1b — customer status is ORDER_DELIVERED
check("Customer status = ORDER_DELIVERED",
      customer.local_state["status"] == "ORDER_DELIVERED",
      "ORDER_DELIVERED", customer.local_state["status"])

# 1c — OrderProcessor status is DELIVERED
check("OrderProcessor status = DELIVERED",
      op.local_state["status"] == "DELIVERED",
      "DELIVERED", op.local_state["status"])

# 1d — final customer VC is [4,7,7,8]
final_vc = customer.vc.get()
check("Final Customer VC = [4,7,7,8]",
      final_vc == [4, 7, 7, 8],
      "[4, 7, 7, 8]", str(final_vc))

# ─────────────────────────────────────────────────────────────────
# TEST CASE 2 — Vector clock correctness
# ─────────────────────────────────────────────────────────────────
print(f"\n{YELLOW}Test Case 2 — Vector clock update rules are correct{RESET}")

# 2a — after Customer's first internal event: [1,0,0,0]
e_cust_internal = next(e for e in customer.event_log
                       if "decides to order" in e["event"])
check("Customer first internal event VC = [1,0,0,0]",
      e_cust_internal["vc"] == [1, 0, 0, 0],
      "[1, 0, 0, 0]", str(e_cust_internal["vc"]))

# 2b — after Customer sends ORDER: [2,0,0,0]
e_cust_send = next(e for e in customer.event_log if "SEND" in e["event"])
check("Customer SEND ORDER VC = [2,0,0,0]",
      e_cust_send["vc"] == [2, 0, 0, 0],
      "[2, 0, 0, 0]", str(e_cust_send["vc"]))

# 2c — after OrderProcessor receives ORDER: [2,1,0,0]
e_op_recv = next(e for e in op.event_log if "RECV" in e["event"] and "ORDER" in e["event"])
check("OrderProcessor RECV ORDER VC = [2,1,0,0]",
      e_op_recv["vc"] == [2, 1, 0, 0],
      "[2, 1, 0, 0]", str(e_op_recv["vc"]))

# 2d — after Restaurant receives ASSIGN: [2,3,3,0]
e_rest_recv = next(e for e in restaurant.event_log if "RECV" in e["event"] and "ASSIGN" in e["event"])
check("Restaurant RECV ASSIGN VC = [2,3,3,0]",
      e_rest_recv["vc"] == [2, 3, 3, 0],
      "[2, 3, 3, 0]", str(e_rest_recv["vc"]))

# 2e — after DeliveryPartner receives PICKUP: [2,5,7,3]
e_dp_recv = next(e for e in delivery.event_log if "RECV" in e["event"] and "PICKUP" in e["event"])
check("DeliveryPartner RECV PICKUP VC = [2,5,7,3]",
      e_dp_recv["vc"] == [2, 5, 7, 3],
      "[2, 5, 7, 3]", str(e_dp_recv["vc"]))

# ─────────────────────────────────────────────────────────────────
# TEST CASE 3 — Concurrent events detected
# ─────────────────────────────────────────────────────────────────
print(f"\n{YELLOW}Test Case 3 — Concurrent events are detected{RESET}")

result = analyse(all_events)
concurrent_pairs = result["concurrent"]

# 3a — at least 1 concurrent pair found
check("At least 1 concurrent pair found",
      len(concurrent_pairs) >= 1,
      ">= 1", str(len(concurrent_pairs)))

# 3b — exactly 38 concurrent pairs
check("38 concurrent pairs found",
      len(concurrent_pairs) == 38,
      "38", str(len(concurrent_pairs)))

# 3c — verify Restaurant opens kitchen vs DeliveryPartner checks vehicle are concurrent
rest_kitchen = next((e for e in all_events if "opens kitchen" in e["event"]), None)
dp_vehicle   = next((e for e in all_events if "checks vehicle" in e["event"]), None)
if rest_kitchen and dp_vehicle:
    is_conc = VectorClock.concurrent(rest_kitchen["vc"], dp_vehicle["vc"])
    check("Restaurant opens kitchen || DeliveryPartner checks vehicle",
          is_conc,
          "concurrent", "not concurrent" if not is_conc else "concurrent")
else:
    check("Restaurant/DeliveryPartner events found", False, "events exist", "events missing")

# 3d — verify Restaurant and DeliveryPartner early events have no causal link
check("Restaurant [0,0,1,0] NOT happened-before DeliveryPartner [0,0,0,1]",
      not VectorClock.happened_before([0, 0, 1, 0], [0, 0, 0, 1]),
      "False", str(VectorClock.happened_before([0, 0, 1, 0], [0, 0, 0, 1])))

check("DeliveryPartner [0,0,0,1] NOT happened-before Restaurant [0,0,1,0]",
      not VectorClock.happened_before([0, 0, 0, 1], [0, 0, 1, 0]),
      "False", str(VectorClock.happened_before([0, 0, 0, 1], [0, 0, 1, 0])))

# ─────────────────────────────────────────────────────────────────
# TEST CASE 4 — All 4 processes record state in snapshot
# ─────────────────────────────────────────────────────────────────
print(f"\n{YELLOW}Test Case 4 — All 4 processes record state in snapshot{RESET}")

snapshot = collect(processes)

# 4a — all 4 process states recorded
check("All 4 process states recorded",
      len(snapshot["process_states"]) == 4,
      "4", str(len(snapshot["process_states"])))

# 4b — each process has a non-empty recorded state
for p in processes:
    check(f"{p.name} has recorded state",
          bool(p.snapshot_recorded_state),
          "non-empty dict", "empty")

# 4c — snapshot VCs match expected values
expected_vcs = {
    "Customer":        [2, 0, 0, 0],
    "OrderProcessor":  [2, 4, 7, 0],
    "Restaurant":      [2, 3, 7, 0],
    "DeliveryPartner": [2, 5, 7, 3],
}
for name, expected_vc in expected_vcs.items():
    actual_vc = snapshot["process_states"][name]["vc"]
    check(f"{name} snapshot VC = {expected_vc}",
          actual_vc == expected_vc,
          str(expected_vc), str(actual_vc))

# ─────────────────────────────────────────────────────────────────
# TEST CASE 5 — Snapshot is consistent
# ─────────────────────────────────────────────────────────────────
print(f"\n{YELLOW}Test Case 5 — Captured global snapshot is consistent{RESET}")

is_consistent, explanation = check_consistency(snapshot)

# 5a — consistency check returns True
check("Snapshot is CONSISTENT",
      is_consistent,
      "True", str(is_consistent))

# 5b — all channels are empty (no in-transit messages)
in_transit_count = sum(
    len(msgs)
    for channels in snapshot["channel_states"].values()
    for msgs in channels.values()
)
check("0 in-transit messages at snapshot time",
      in_transit_count == 0,
      "0", str(in_transit_count))

# 5c — 12 channels recorded (all directed pairs of 4 processes)
total_channels = sum(len(ch) for ch in snapshot["channel_states"].values())
check("12 channels recorded",
      total_channels == 12,
      "12", str(total_channels))

# ─────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────
total = passed + failed
print(f"\n{'=' * 50}")
print(f"  Results: {GREEN}{passed} passed{RESET}  |  {RED}{failed} failed{RESET}  |  {total} total")
print(f"{'=' * 50}\n")

sys.exit(0 if failed == 0 else 1)
