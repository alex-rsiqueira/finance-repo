"""Reads Millennium BCP monthly statements (EXTRATO COMBINADO PDFs) into JSON lines
for raw.tb_bank_statement_lines (finance-backend plans/bank-feed-in-the-app.md §3.4).

Only the current account's section (CONTA MILLENNIUM) is read. Every line is checked
against the statement itself: its amount must equal the change in the printed balance,
the last balance must equal SALDO FINAL, and each statement must open at the previous
one's closing balance. Any mismatch stops the run, so a file that reads wrongly never
gets loaded.

    python3 millennium_statements.py <folder with the PDFs> <household id> <output.jsonl>

then load the file:

    bq --project_id=finances-314506 load --source_format=NEWLINE_DELIMITED_JSON \
      raw.tb_bank_statement_lines <output.jsonl>

A line keeps its line_id whatever file it came from (D-17), and staging keeps one
row per line, so loading a statement twice adds nothing.

Needs pdftotext (poppler).
"""
import glob
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

folder, household, output = sys.argv[1], sys.argv[2], sys.argv[3]
NUM = r"\d{1,3}(?: \d{3})*\.\d{2}"
LINE = re.compile(rf"^\s*(\d{{1,2}})\.(\d{{2}})\s+(\d{{1,2}})\.(\d{{2}})\s+(.*?)\s+({NUM})\s+({NUM})( -)?\s*$")
DATED = re.compile(r"^\s*\d{1,2}\.\d{2}\s+\d{1,2}\.\d{2}\s")

rows, previous_close = [], None
now = datetime.now(timezone.utc)
dtinsert = now.strftime("%Y-%m-%d %H:%M:%S")
import_id = "millennium_pdf_" + now.strftime("%Y%m%dT%H%M%S")
seen = {}
for path in sorted(glob.glob(os.path.join(folder, "*.pdf"))):
    text = subprocess.run(["pdftotext", "-layout", path, "-"], capture_output=True, text=True, check=True).stdout
    section = text[text.index("CONTA MILLENNIUM"):]
    section = section[:section.index("\n", section.index("SALDO FINAL"))]
    account = re.search(r"CONTA MILLENNIUM\s+N\. (\d+)", section)[1]
    iban = re.search(r"IBAN: (PT[\d ]+)", text)[1].replace(" ", "")
    statement = re.search(r"EXTRATO COMBINADO\s+N\. (\d{4}/\d{3})", text)[1]
    period = re.search(r"EXTRATO DE (\d{4})/(\d{2})/\d{2} A (\d{4})/\d{2}/\d{2}", section)
    first_year, first_month, last_year = int(period[1]), int(period[2]), int(period[3])
    opening = re.search(rf"SALDO INICIAL\s+({NUM})( -)?", section)
    closing = re.search(rf"SALDO FINAL\s+({NUM})( -)?", section)
    balance = round((-1 if opening[2] else 1) * float(opening[1].replace(" ", "")), 2)
    close = round((-1 if closing[2] else 1) * float(closing[1].replace(" ", "")), 2)
    if previous_close is not None and balance != previous_close:
        sys.exit(f"{statement}: opens at {balance}, the previous statement closed at {previous_close}")

    line_no = 0
    for line in section.splitlines():
        if not DATED.match(line):
            continue
        match = LINE.match(line)
        if not match:
            sys.exit(f"{statement}: cannot read line {line.strip()!r}")
        month, day, value_month, value_day, description, amount, printed, negative = match.groups()
        new_balance = round((-1 if negative else 1) * float(printed.replace(" ", "")), 2)
        change = round(new_balance - balance, 2)
        if abs(change) != float(amount.replace(" ", "")):
            sys.exit(f"{statement}: {line.strip()!r} does not match the balance change {change}")
        year = first_year if int(month) == first_month else last_year
        # The value date can fall in the year before or after (booked 12.31, value 1.02).
        value_year = year - 1 if int(value_month) > int(month) + 6 else year + 1 if int(value_month) < int(month) - 6 else year
        line_no += 1
        booking = f"{year}-{int(month):02d}-{int(day):02d}"
        key = f"{iban}|{booking}|{change:.2f}|{new_balance:.2f}"
        seen[key] = seen.get(key, 0) + 1
        rows.append({
            "household_id": household,
            "line_id": hashlib.md5(f"{key}|{seen[key]}".encode()).hexdigest(),
            "bank": "MILLENNIUMBCP",
            "iban": iban,
            "account_number": account,
            "statement": statement,
            "line_no": line_no,
            "booking_date": booking,
            "value_date": f"{value_year}-{int(value_month):02d}-{int(value_day):02d}",
            "description": re.sub(r"\s+", " ", description).strip(),
            "amount": f"{change:.2f}",
            "balance": f"{new_balance:.2f}",
            "currency": "EUR",
            "source_file": os.path.basename(path),
            "import_id": import_id,
            "dtinsert": dtinsert,
        })
        balance = new_balance
    if balance != close:
        sys.exit(f"{statement}: lines end at {balance}, SALDO FINAL is {close}")
    previous_close = close
    print(f"{statement}  {line_no:4d} lines  closes at {close:.2f}")

with open(output, "w", encoding="utf-8") as f:
    for row in rows:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
print(f"{len(rows)} lines written to {output}")
