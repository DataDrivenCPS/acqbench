"""Extract SPARQL queries logged by acquirium.graph_store from .run_logs."""
import re, json, hashlib, pathlib, collections, sys

LOGDIR = pathlib.Path("/Users/umsaka/Documents/DDCPS/NAWI/acquirium/.run_logs")
# a new log record starts with a timestamp at column 0
TS = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} ")
HEAD = re.compile(r"DEBUG acquirium\.graph_store sparql_query union=(True|False) query=(.*)$")

def extract(path):
    out = []
    cur = None
    with open(path, "r", errors="replace") as fh:
        for line in fh:
            if TS.match(line):
                if cur is not None:
                    out.append(cur); cur = None
                m = HEAD.search(line)
                if m:
                    cur = {"union": m.group(1) == "True",
                           "lines": [m.group(2).rstrip("\n")],
                           "src": path.name}
            elif cur is not None:
                cur["lines"].append(line.rstrip("\n"))
    if cur is not None:
        out.append(cur)
    for q in out:
        q["query"] = "\n".join(q.pop("lines")).strip()
    return out

def main():
    # DPR-only logs: mention "dpr" and never "watertap"
    dpr_logs = []
    for p in sorted(LOGDIR.iterdir()):
        if p.suffix not in (".log", ".txt"):
            continue
        blob = p.read_text(errors="replace").lower()
        if "dpr" in blob and "watertap" not in blob:
            dpr_logs.append(p)
    print("DPR logs:", ", ".join(x.name for x in dpr_logs))

    allq = []
    for p in dpr_logs:
        if True:
            try:
                allq.extend(extract(p))
            except Exception as e:
                print(f"skip {p.name}: {e}", file=sys.stderr)

    # dedupe on exact query text, keep counts + which logs it came from
    by_text = collections.OrderedDict()
    for q in allq:
        key = (q["query"], q["union"])
        e = by_text.setdefault(key, {"query": q["query"], "union": q["union"],
                                     "count": 0, "srcs": collections.Counter()})
        e["count"] += 1
        e["srcs"][q["src"]] += 1

    uniq = []
    for e in by_text.values():
        e["srcs"] = dict(e["srcs"])
        e["id"] = hashlib.sha1(e["query"].encode()).hexdigest()[:10]
        e["nlines"] = e["query"].count("\n") + 1
        e["nchars"] = len(e["query"])
        uniq.append(e)
    uniq.sort(key=lambda e: -e["count"])

    print(f"total logged queries: {len(allq)}")
    print(f"unique query texts:   {len(uniq)}")
    outp = pathlib.Path(__file__).parent / "extracted_queries.json"
    outp.write_text(json.dumps(uniq, indent=2))
    print(f"wrote {outp}")
    print("\ntop 25 by frequency:")
    for e in uniq[:25]:
        first = e["query"].split("\n")[0][:95]
        print(f"  {e['count']:6d}  union={str(e['union']):5s} {e['nchars']:6d}ch  {e['id']}  {first}")

main()
