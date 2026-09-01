"""Text report for the mutual-fund overlap analysis in `helpers/mf_overlap.py`.

The dashboard serves the same analysis at /api/mf/overlap and renders it in the
Playground tab; this is the terminal view of it.

    python3 scripts/mf_overlap.py                # report
    python3 scripts/mf_overlap.py --json out.json
    python3 scripts/mf_overlap.py --refresh      # re-fetch fund portfolios
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers import mf_overlap

ACCOUNT_LABELS = {"1": "Partha", "2": "Soumita", "3": "Dad", "4": "Mom"}


def label(acct):
    return ACCOUNT_LABELS.get(acct, "Account " + str(acct))


def rupees(v):
    return "Rs {:,.0f}".format(v)


def report(data):
    print("=" * 78)
    print("MUTUAL FUND LOOK-THROUGH OVERLAP")
    print("=" * 78)

    for acct in data["accounts"]:
        print("\n--- {} ({} funds, {}; equity {}) ---".format(
            label(acct["account"]), acct["fundCount"],
            rupees(acct["totalInr"]), rupees(acct["equityInr"])))
        for f in acct["funds"]:
            tag = f["category"] if f["isEquity"] else f["category"] + " [no equity]"
            stk = "{:>3} stk".format(f["stockCount"]) if f["isEquity"] else "       "
            print("  {:>16}  {:<28} {}  {}".format(
                rupees(f["valueInr"]), tag, stk, f["name"]))

        if acct["pairs"]:
            print("  overlap between funds held here:")
            for p in acct["pairs"][:12]:
                print("    {:>5.1f}%  {:>3} shared  {}  vs  {}".format(
                    p["overlapPct"], p["sharedStocks"], p["a"][:32], p["b"][:32]))

        if acct["topStocks"]:
            print("  top look-through stock exposure ({} distinct, effective {}):".format(
                acct["distinctStocks"], acct["effectiveStocks"]))
            for s in acct["topStocks"][:8]:
                print("    {:>14}  {:>5.2f}% of equity  via {} fund(s)  {}".format(
                    rupees(s["inr"]), s["pct"], s["fundCount"], s["name"]))

    print("\n" + "=" * 78)
    print("HOUSEHOLD (all accounts combined)")
    print("=" * 78)
    print("equity {} across {} distinct stocks (concentration equivalent to {} "
          "equally weighted names)".format(rupees(data["equityTotalInr"]),
                                           data["distinctStocks"], data["effectiveStocks"]))
    print("debt / liquid (no equity, excluded from overlap): {}".format(rupees(data["debtTotalInr"])))

    print("\ntop single-name exposures:")
    for s in data["household"][:15]:
        print("  {:>14}  {:>5.2f}%  {:>2} funds  {}".format(
            rupees(s["inr"]), s["pct"], s["fundCount"], s["name"]))

    if data["crossAccount"]:
        print("\nhighest overlap across different accounts:")
        for p in data["crossAccount"][:10]:
            print("  {:>5.1f}%  {} ({})  vs  {} ({})".format(
                p["overlapPct"], p["a"][:28], label(p["accountA"]),
                p["b"][:28], label(p["accountB"])))

    if data["duplicateSchemes"]:
        print("\nsame scheme held in multiple accounts:")
        for d in data["duplicateSchemes"]:
            print("  {:>14}  {}  ->  {}".format(
                rupees(d["totalInr"]), d["name"], ", ".join(label(a) for a in d["accounts"])))

    print("\ntop sectors (household equity):")
    for s in data["sectors"][:10]:
        print("  {:>14}  {:>5.2f}%  {}".format(rupees(s["inr"]), s["pct"], s["sector"]))

    if data["unavailable"]:
        print("\nno portfolio available (excluded):")
        for u in data["unavailable"]:
            print("  {} ({})".format(u["schemeName"], u["schemeCode"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="re-fetch portfolios, ignoring cache")
    ap.add_argument("--json", metavar="PATH", help="also write the full analysis as JSON")
    args = ap.parse_args()

    data = mf_overlap.compute(refresh=args.refresh)
    report(data)

    if args.json:
        with open(args.json, "w") as f:
            json.dump(data, f, indent=2)
        print("\nwrote {}".format(args.json))


if __name__ == "__main__":
    main()
