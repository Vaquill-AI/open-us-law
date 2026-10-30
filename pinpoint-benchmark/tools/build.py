"""Build the release draft from the bench inputs. Maintainer tool, not needed by customers.

Truth per section: mn_adj (B adjudication) > mn_redo (A by eye) > other B > other A.
Reads only local bench files; makes no network call.
"""
import glob, gzip, hashlib, json, re, sys, collections
BENCH = "/private/tmp/claude-501/p/bench/"
OUT = "/private/tmp/claude-501/p/release_pinpoint/"
STATES = ["mn", "ia", "mo", "wi", "mi"]
SCORING_EXCLUDED = set()   # the two former exclusions (MN:35.01, MN:51A.02) were relabelled by eye (mn_redo) and are scored
IA_KEY_FIX = {"STATE_IA_TV_C654A_S654A.7": "STATE_IA_TXV_C654A_S654A.7"}  # A file used a stale title code

def load(d):
    L, src = {}, {}
    files = sorted(glob.glob(f"{BENCH}labels/{d}/*.json"),
                   key=lambda x: (x.endswith(("redo.json", "adj.json")), x))
    for f in files:
        for k, v in json.load(open(f)).items():
            k = IA_KEY_FIX.get(k, k)
            L[k] = v
            src[k] = f.split("/")[-1][:-5]
    return L, src

def public_id(act_id):
    m = re.match(r"STATE_([A-Z]{2})_.*_S(.+)$", act_id)
    return f"{m.group(1)}:{m.group(2)}"

LABEL_RE = re.compile(r"(?:(?:Subdivision|Subd\.|Subsection)\s*)?(?:\([0-9A-Za-z]+\)[\s  ]*)+$"
                      r"|(?:(?:Subdivision|Subd\.|Subsection)\s*)?[0-9A-Za-z]{1,4}\.[\s  ]*$")

def locate(text, nodes):
    """Add start/end offsets by finding each verbatim head in document order."""
    pos, prev = 0, -1
    starts = []
    for n in nodes:
        h = re.sub(r"\s+", " ", n["head"]).strip()
        words = h.split(" ")
        found = -1
        # whitespace-tolerant search
        for take in (len(words), min(len(words), 4)):
            pat = r"\s+".join(re.escape(w) for w in words[:take])
            for frm in (prev + 1, prev if prev >= 0 else 0):
                if frm < 0: frm = 0
                m = re.compile(pat).search(text, frm)
                if m:
                    found = m.start(); break
            if found >= 0: break
        if not h:
            starts.append("EMPTY"); continue
        if found < 0:
            starts.append(None); continue
        prev = found
        ls = found
        m = LABEL_RE.search(text[max(0, found - 24):found])
        if m: ls = found - (len(text[max(0, found - 24):found]) - m.start())
        starts.append((ls, found))
    for i in range(len(starts) - 1, -1, -1):   # empty head: own text begins where the next node begins
        if starts[i] == "EMPTY":
            nxt = starts[i + 1] if i + 1 < len(starts) else None
            starts[i] = nxt if isinstance(nxt, tuple) else None
    out = []
    body_end = len(text.rstrip())
    hm = list(re.finditer(r"\n\n(?:History|HISTORY|Source|SOURCE|Credit)[:.]", text))
    if hm: body_end = hm[-1].start()
    for i, n in enumerate(nodes):
        s = starts[i]
        e = None
        if s is not None:
            e = body_end
            for j in range(i + 1, len(nodes)):
                if not nodes[j]["path"].startswith(n["path"] + "/") and starts[j] is not None:
                    e = starts[j][0]; break
            e = max(e, s[1])
        out.append({"path": n["path"], "start": s[0] if s else None, "end": e, "head": n["head"]})
    return out

def main():
    A, srcA = load("A"); B, srcB = load("B")
    stats = collections.defaultdict(lambda: collections.Counter())
    unlocated = collections.Counter()
    ids_seen = set()
    for st in STATES:
        sample = json.load(open(f"{BENCH}sample_{st}.json"))
        recs = []
        for r in sorted(sample, key=lambda r: public_id(r["act_id"])):
            a = r["act_id"]
            la, lb = A[a], B.get(a)
            if srcB.get(a) == "mn_adj": truth = lb
            elif srcA[a] == "mn_redo": truth = la
            else: truth = lb if lb is not None else la
            pid = public_id(a)
            assert pid not in ids_seen, pid
            ids_seen.add(pid)
            eng = [n["path"] for n in r["engine"]]
            pa = [n["path"] for n in la]
            S = stats[st]
            S["n"] += 1
            S["A_eq_first_engine"] += pa == eng
            if pa != eng:
                S["disagreements"] += 1
                if lb is not None:
                    S["B_labelled"] += 1
                    S["B_eq_A"] += [n["path"] for n in lb] == pa
                    S["B_eq_engine"] += [n["path"] for n in lb] == eng
            S["truth_eq_first_engine"] += [n["path"] for n in truth] == eng
            S["nodes"] += len(truth)
            if lb is not None and [n["path"] for n in lb] != pa:
                S["B_differs_from_A"] += 1
                S["B_differs_and_adjudicated"] += srcB[a] == "mn_adj"
            outline = locate(r["plain"], truth)
            for o in outline:
                if o["start"] is None: unlocated[st] += 1
            labeller = "B_adjudicated" if srcB.get(a) == "mn_adj" else "A_redo_by_eye" if srcA[a] == "mn_redo" else "B" if lb is not None else "A"
            rec = {"id": pid, "state": st.upper(), "stratum": r["stratum"],
                   "truth_source": labeller, "scoring_excluded": pid in SCORING_EXCLUDED,
                   "text_chars": len(r["plain"]), "outline": outline}
            recs.append((rec, r["plain"]))
        for variant, d in (("full", "data"), ("outline", "data_outline_only")):
            fn = f"{OUT}{d}/pinpoint_{st}.jsonl.gz"
            with gzip.GzipFile(fn, "wb", mtime=0) as g:
                for rec, text in recs:
                    row = dict(rec)
                    if variant == "full": row["text"] = text
                    g.write((json.dumps(row, ensure_ascii=False, sort_keys=False) + "\n").encode())
    json.dump({k: dict(v) for k, v in stats.items()}, open(OUT + "tools/stats.json", "w"), indent=1)
    print(json.dumps({k: dict(v) for k, v in stats.items()}, indent=1)); print("unlocated heads", dict(unlocated))

main()
