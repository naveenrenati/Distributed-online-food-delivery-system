"""
concurrent_events.py
--------------------
Analyses the event log and reports:
  - Happened-before pairs  (event A causally preceded event B)
  - Concurrent pairs       (neither event preceded the other)
"""

from vector_clock import VectorClock


def analyse(events: list) -> dict:
    """
    Compare every pair of events.
    Returns dict with keys 'happened_before' and 'concurrent',
    each a list of (i, j) index pairs.
    """
    hb_pairs  = []
    con_pairs = []

    for i in range(len(events)):
        for j in range(i + 1, len(events)):
            vi, vj = events[i]["vc"], events[j]["vc"]
            if VectorClock.happened_before(vi, vj):
                hb_pairs.append((i, j))
            elif VectorClock.happened_before(vj, vi):
                hb_pairs.append((j, i))
            else:
                con_pairs.append((i, j))

    return {"happened_before": hb_pairs, "concurrent": con_pairs}


def print_concurrency_report(events: list):
    result = analyse(events)
    hb     = result["happened_before"]
    cp     = result["concurrent"]

    print("\n" + "=" * 70)
    print("  CONCURRENCY ANALYSIS REPORT")
    print("=" * 70)

    # Show first 10 happened-before pairs
    print(f"\n  Happened-Before relationships: {len(hb)} pairs found")
    for i, j in hb[:10]:
        e1, e2 = events[i], events[j]
        print(f"    [{e1['process']}] \"{e1['event'][:40]}\" VC={e1['vc']}")
        print(f"      →  [{e2['process']}] \"{e2['event'][:40]}\" VC={e2['vc']}")
    if len(hb) > 10:
        print(f"    ... and {len(hb) - 10} more.")

    # Show all concurrent pairs with verification
    print(f"\n  CONCURRENT EVENT PAIRS: {len(cp)} pair(s) found")
    print("  (Neither event happened-before the other)\n")

    if not cp:
        print("  No concurrent events detected.")
        return

    for idx, (i, j) in enumerate(cp):
        e1, e2 = events[i], events[j]
        v1, v2 = e1["vc"], e2["vc"]
        print(f"  Concurrent Pair #{idx + 1}:")
        print(f"    Process : {e1['process']}")
        print(f"    Event   : {e1['event']}")
        print(f"    VC      : {v1}")
        print(f"    ---")
        print(f"    Process : {e2['process']}")
        print(f"    Event   : {e2['event']}")
        print(f"    VC      : {v2}")
        print(f"")
        print(f"    Verification:")
        print(f"      {v1} ≤ {v2}? {'NO' if not VectorClock.happened_before(v1, v2) else 'YES'}"
              f" → Event-1 did NOT happen-before Event-2")
        print(f"      {v2} ≤ {v1}? {'NO' if not VectorClock.happened_before(v2, v1) else 'YES'}"
              f" → Event-2 did NOT happen-before Event-1")
        print(f"    ✓ CONFIRMED CONCURRENT\n")

    print("=" * 70)
