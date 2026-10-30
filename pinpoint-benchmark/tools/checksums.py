import hashlib, json, os
R = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
files = ["score.py", "verify.py", "README.md", "COUNTS.json", "LICENSE-DATA.md"] + \
        [f"{d}/pinpoint_{s}.jsonl.gz" for d in ("data", "data_outline_only") for s in ("mn", "ia", "mo", "wi", "mi")]
out = {f: hashlib.sha256(open(os.path.join(R, f), "rb").read()).hexdigest() for f in files if os.path.exists(os.path.join(R, f))}
json.dump(out, open(os.path.join(R, "SHA256SUMS.json"), "w"), indent=1, sort_keys=True)
print(len(out), "files")
