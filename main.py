"""
main.py
-------
Simulation entry point.

Starts 4 process threads, waits for them to finish,
then prints the event timeline, concurrency report, and snapshot report.

Process IDs:
  0 — Customer
  1 — OrderProcessor
  2 — Restaurant
  3 — DeliveryPartner
"""

import threading
from customer        import Customer
from order_processor import OrderProcessor
from restaurant      import Restaurant
from delivery_partner import DeliveryPartner
from concurrent_events  import print_concurrency_report
from snapshot_collector import print_snapshot_report

N = 4   # total number of processes


def banner(text: str):
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)


def main():
    banner("DISTRIBUTED FOOD DELIVERY SYSTEM — CCZG 526 Lab Assignment I")
    print("""
  Processes:
    [0] Customer         — places orders, receives delivery confirmation
    [1] OrderProcessor   — central coordinator, initiates snapshot
    [2] Restaurant       — receives assignment, prepares food
    [3] DeliveryPartner  — picks up and delivers orders

  Algorithms:
    • Vector Clocks       (Lamport 1978)
    • Chandy-Lamport Global Snapshot (1985)
""")

    # Create processes
    processes = [
        Customer(0, N),
        OrderProcessor(1, N),
        Restaurant(2, N),
        DeliveryPartner(3, N),
    ]

    # Start each process in its own thread
    banner("STARTING SIMULATION")
    threads = [threading.Thread(target=p.run, name=p.name) for p in processes]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    # Merge and sort all events by total VC sum (approximate causal order)
    all_events = sorted(
        [e for p in processes for e in p.event_log],
        key=lambda e: sum(e["vc"])
    )

    # Print event timeline
    banner("COMPLETE EVENT TIMELINE (sorted by total VC sum)")
    print(f"  {'#':<4} {'Process':<18} {'Event':<50} {'VectorClock'}")
    print("  " + "-" * 90)
    for idx, ev in enumerate(all_events):
        print(f"  {idx:<4} {ev['process']:<18} {ev['event'][:48]:<50} {ev['vc']}")

    # Concurrency analysis
    print_concurrency_report(all_events)

    # Snapshot report
    print_snapshot_report(processes)

    # Summary
    banner("SIMULATION COMPLETE")
    print(f"  Total events logged : {len(all_events)}")
    print(f"  Processes           : {[p.name for p in processes]}")
    print(f"  Snapshot recorded   : {'YES' if processes[1].snapshot_recorded_state else 'NO'}")
    print()


if __name__ == "__main__":
    main()
