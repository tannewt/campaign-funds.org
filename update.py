import csv
import pathlib
import urllib.request
import progressbar
from datetime import datetime
import dateutil.parser
import functools

build = pathlib.Path("build")

source_csvs = {
    "contributions.csv": "https://data.wa.gov/api/views/kv7h-kjye/rows.csv?accessType=DOWNLOAD",
    "registrations.csv": "https://data.wa.gov/api/views/iz23-7xxj/rows.csv?accessType=DOWNLOAD",
    "expenditures.csv": "https://data.wa.gov/api/views/tijg-9zyp/rows.csv?accessType=DOWNLOAD"
}

for source in source_csvs:
    if not (build / source).exists():
        print(source)
        urllib.request.urlretrieve(source_csvs[source], build / source)

# post-process

import sqlite3
con = sqlite3.connect('raw.db')
c = con.cursor()

INT_ROWS = ("id", "report_number", "committee_zip", "party_code", "committee_id", "election_year", "recipient_zip", "treasurer_zip", "contributor_zip")
DATE_ROWS = ("expenditure_date", "receipt_date")
FLOAT_ROWS = ("amount",)
EXTRACT_ROWS = ("party", "filer_type", "origin", "filer_name", "type", "cash_or_in_kind", "code", "contributor_category", "primary_general", "itemized_or_non_itemized")

PRINT_BAD_VALUES = False

@functools.lru_cache(len(EXTRACT_ROWS) * 16)
def get_id(table, field, value):
    c.execute(f"SELECT id FROM [{table}_{field}] WHERE [{field}]=?", (value,))
    r = c.fetchone()
    if r is None:
        c.execute(f"INSERT INTO [{table}_{field}]([{field}]) VALUES (?)", (value,))
        c.execute(f"SELECT id FROM [{table}_{field}] WHERE [{field}]=?", (value,))
        r = c.fetchone()
    return r[0]

@functools.lru_cache(len(DATE_ROWS) * 256)
def parse_date(value):
    d = dateutil.parser.parse(value)
    if d is None:
        print(value)
    return d.date().isoformat()

for filename in source_csvs:
    path = build / filename
    print(filename)
    with path.open(newline='') as f:
        reader = csv.reader(f)
        row_count = 0
        for row in reader:
            row_count += 1
        print(row_count)
        f.seek(0)
        reader = csv.DictReader(f)
        fields = []
        inserts = []
        cols = []
        table = filename.split(".")[0]
        for field in reader.fieldnames:
            t = "TEXT"
            col = field
            if field in INT_ROWS or field in DATE_ROWS:
                t = "INTEGER"
            elif field in FLOAT_ROWS:
                t = "REAL"
            elif field in EXTRACT_ROWS:
                con.execute(f"CREATE TABLE [{table}_{field}] ([id] INTEGER PRIMARY KEY, [{field}] TEXT)")
                con.execute(f"CREATE UNIQUE INDEX [idx_{table}_{field}_{field}] ON [{table}_{field}] ([{field}])")
                t = f"INTEGER REFERENCES [{table}_{field}]([id])"
                col = f"{field}_id"
            fields.append(f"[{col}] {t}")
            cols.append(f"[{col}]")
            inserts.append(f":{field}")
        fields = ", ".join(fields)
        cols = ", ".join(cols)
        inserts = ", ".join(inserts)
        #print(f"CREATE TABLE [{table}] ([rowid] INTEGER PRIMARY KEY, {fields})")
        con.execute(f"CREATE TABLE [{table}] ([rowid] INTEGER PRIMARY KEY, {fields})")
        con.commit()
        insert_sql = f"INSERT INTO [{table}]({cols}) VALUES ({inserts})"
        i = 0
        with progressbar.ProgressBar(max_value=row_count, redirect_stdout=True) as bar:
            for row in bar(reader):
                for k in row:
                    try:
                        if row[k] == '':
                            row[k] = None
                        elif k in INT_ROWS:
                            row[k] = int(row[k])
                        elif k in FLOAT_ROWS:
                            row[k] = float(row[k])
                        elif k in DATE_ROWS:
                            row[k] = parse_date(row[k])
                        elif k in EXTRACT_ROWS:
                            row[k] = get_id(table, k, row[k])

                        # print(k, repr(row[k]))
                    except ValueError as e:
                        if PRINT_BAD_VALUES:
                            print(k, row[k], e)
                con.execute(insert_sql, row)
                if i % 10000 == 0:
                    con.commit()
                i += 1
                #break

con.execute("CREATE TABLE contribution_totals AS SELECT contributor_name, election_year, SUM(amount) as total_amount FROM contributions GROUP BY contributor_name, election_year")
con.commit()

con.commit()
con.close()
