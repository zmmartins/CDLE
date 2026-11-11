"""Check a lab report and pack it into the zip you upload to Moodle.

  python3 pack.py lab1 51234 51987      a group of two (student numbers)
  python3 pack.py lab1 51234            one student

What each lab needs is described in labs/<lab>.json, which comes with the lab's report part
(<lab>_report_template.zip): the section titles, the AI sessions, and the code files to hand in.

It reads, in this folder: report_<lab>.md, report_<lab>.pdf, ai-log-<lab>.md (if the lab has an AI session),
evidence/<lab>/ and the figures that the report uses, plus the code files the lab names (in the lab's own
folder, e.g. ../lab2/plans.py). It writes <lab>_<numbers>.zip (numbers in increasing order) and prints what
it checked. ERROR means the zip was not written; WARNING means it was written, but fix it if you can.
Only the Python standard library is used (pypdf is used to count pages if it is installed).
"""
import json
import os
import re
import sys
import zipfile

LIMIT_PAGES = 4
FORBIDDEN = re.compile(r"(\.dat|\.parquet|\.tpl|query_0\.sql|query\d\d\.sql)$", re.I)
SKIP = {".keep", ".DS_Store"}

errors, warnings = [], []


def norm(title):
    return re.sub(r"\s+", "", title).lower()


def count_pages(path):
    try:
        import pypdf
        return len(pypdf.PdfReader(path).pages)
    except Exception:
        pass
    data = open(path, "rb").read()
    counts = [int(c) for c in re.findall(rb"/Type\s*/Pages\b[^>]*?/Count\s+(\d+)", data, re.S)]
    if counts:
        return max(counts)
    pages = len(re.findall(rb"/Type\s*/Page\b(?![sL])", data))
    return pages or None


def section_body(text, heading):
    """The text under a '## heading' of an AI log, without comments and blank lines."""
    m = re.search(rf"^##\s+{re.escape(heading)}\b.*?$(.*?)(?=^##\s|\Z)", text, re.M | re.S)
    if not m:
        return None
    body = re.sub(r"<!--.*?-->", "", m.group(1), flags=re.S)
    return [l for l in body.splitlines()
            if l.strip() and not l.startswith("###") and not re.match(r"\*\*(Date|Assistant and version):\*\*\s*$", l.strip())]


def files_under(folder):
    out = []
    for root, dirs, names in os.walk(folder):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for n in sorted(names):
            if n not in SKIP and not n.endswith(".pyc"):
                out.append(os.path.join(root, n))
    return out


def code_files(lab, paths):
    """(file on disk, name in the zip) for the code the lab asks for, e.g. ../lab2/plans.py -> code/plans.py."""
    out = []
    base = os.path.normpath(os.path.join("..", lab))
    for p in paths:
        if os.path.isdir(p):
            out += [(f, "code/" + os.path.relpath(f, base)) for f in files_under(p)]
        elif os.path.isfile(p):
            out.append((p, "code/" + os.path.relpath(p, base)))
        else:
            warnings.append(f"{p} not found (the lab asks you to hand it in)")
    return out


def main():
    if len(sys.argv) < 3 or not re.fullmatch(r"lab\d+", sys.argv[1]):
        sys.exit(__doc__)
    lab, numbers = sys.argv[1], sorted(sys.argv[2:])
    if not all(re.fullmatch(r"\d{4,6}", n) for n in numbers) or len(numbers) > 2:
        sys.exit("give one or two student numbers, digits only, e.g.: python3 pack.py lab1 51234 51987")
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    spec_file = f"labs/{lab}.json"
    if not os.path.exists(spec_file):
        sys.exit(f"{spec_file} not found: unzip {lab}_report_template.zip (from Moodle) in ~/tpcds first")
    spec = json.load(open(spec_file, encoding="utf-8"))
    md, pdf = f"report_{lab}.md", f"report_{lab}.pdf"

    # the report source and its header
    text = ""
    if not os.path.exists(md):
        errors.append(f"{md} not found")
    else:
        text = open(md, encoding="utf-8").read()
        header = text.split("---")[1] if text.startswith("---") else ""
        found = sorted(set(re.findall(r"\((\d{4,6})\)", header)))
        if "12345" in found:
            warnings.append(f"{md}: the author lines still have the template's 12345: write your names and numbers")
        if found != numbers:
            warnings.append(f"{md}: student numbers in the header {found or '(none)'} differ from the ones given here {numbers}")
        body = re.sub(r"<!--.*?-->", "", text, flags=re.S)
        headings = [norm(h) for h in re.findall(r"^#\s+(.+?)\s*(?:\{.*\})?\s*$", body, re.M)]
        for h in spec["sections"]:
            if norm(h) not in headings:
                warnings.append(f"{md}: section '{h}' not found (keep the titles of the template)")
        empty = re.findall(r"^#\s+(.+?)\s*$\s*(?=^#\s|\Z)", body, re.M)
        if empty:
            warnings.append(f"{md}: sections with no text: {', '.join(empty)}")

    # the PDF
    if not os.path.exists(pdf):
        errors.append(f"{pdf} not found: run  bash make_pdf.sh {lab}  (or export your PDF with that name)")
    else:
        if os.path.exists(md) and os.path.getmtime(pdf) < os.path.getmtime(md):
            warnings.append(f"{pdf} is older than {md}: run  bash make_pdf.sh {lab}  again")
        pages = count_pages(pdf)
        if pages is None:
            warnings.append(f"{pdf}: could not count the pages; check by hand that there are at most {LIMIT_PAGES}")
        elif pages > LIMIT_PAGES:
            warnings.append(f"{pdf}: {pages} pages; the limit is {LIMIT_PAGES}")
        else:
            print(f"ok   {pdf}: {pages} page(s)")

    # the AI log of this lab (only for labs with an AI session)
    ailog = f"ai-log-{lab}.md" if spec["ai_sessions"] else None
    if ailog and not os.path.exists(ailog):
        errors.append(f"{ailog} not found")
    elif ailog:
        log = open(ailog, encoding="utf-8").read()
        for s in spec["ai_sessions"]:
            lines = section_body(log, s)
            if lines is None:
                warnings.append(f"{ailog}: no '## {s}' entry")
            elif len(lines) < 3:
                warnings.append(f"{ailog}: the '{s}' entry is almost empty")
            else:
                print(f"ok   {ailog}: {s} ({len(lines)} lines)")

    # what goes in the zip: (file on disk, name in the zip)
    members = [(p, p) for p in (pdf, md, ailog) if p and os.path.exists(p)]
    evidence = files_under(f"evidence/{lab}")
    outputs = [e for e in evidence if os.path.basename(e) != "INDEX.md"]
    if not outputs:
        warnings.append(f"evidence/{lab}/ has no outputs: put there the raw outputs your tables come from (see SUBMISSION.md)")
    else:
        index = f"evidence/{lab}/INDEX.md"
        listed = re.sub(r"<!--.*?-->", "", open(index, encoding="utf-8").read(), flags=re.S) if os.path.exists(index) else ""
        unlisted = [os.path.basename(e) for e in outputs if os.path.basename(e) not in listed]
        if unlisted:
            warnings.append(f"{index}: these files are not listed: {', '.join(unlisted[:6])}")
    used = set(re.findall(r"\]\((figures/[^)\s]+)", re.sub(r"<!--.*?-->", "", text, flags=re.S)))
    figures = sorted(f for f in used if os.path.exists(f))
    for f in sorted(used - set(figures)):
        warnings.append(f"{md} uses {f}, which does not exist")
    members += [(e, e) for e in evidence + figures]
    code = code_files(lab, spec.get("code", []))
    for f, _ in code:
        print(f"ok   code: {f}")
    members += code
    bad = [name for f, name in members if FORBIDDEN.search(f)]
    if bad:
        errors.append("TPC-DS data or query files must not be handed in: " + ", ".join(bad[:5]))
    size = sum(os.path.getsize(f) for f, _ in members)
    if size > 20e6:
        warnings.append(f"the zip will hold {size / 1e6:.0f} MB: keep the evidence to the outputs your tables use")

    for w in warnings:
        print("WARNING", w)
    for e in errors:
        print("ERROR  ", e)
    out = f"{lab}_{'_'.join(numbers)}.zip"
    if errors:
        if os.path.exists(out):
            os.remove(out)          # never leave an old zip that could be uploaded by mistake
            print(f"removed the old {out}")
        print("zip not written")
        sys.exit(1)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for f, name in members:
            z.write(f, name)
    print(f"\nwrote {out}: {len(members)} files, {os.path.getsize(out) / 1e6:.1f} MB"
          + (f", {len(warnings)} warning(s)" if warnings else ""))
    for _, name in members:
        print("   ", name)


if __name__ == "__main__":
    main()
