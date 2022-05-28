import sqlite3

import jinja2
import pathlib

loader = jinja2.FileSystemLoader("templates")
jinja_env = jinja2.Environment(loader=loader)


index_data = []

db = sqlite3.connect("raw.db")

# TODO: Load a list of previously rendered filers and contributors so we
# continue to generate them.
filers = set()
contributors = set()

cursor = db.cursor()
cursor.execute("select contributor_name, SUM(amount) as total from contributions group by contributor_name having total > 50000")
for row in cursor.fetchall():
    contributors.add(row[0])

cursor.execute("select filer_id from contributions where election_year >= 2020 group by filer_id")
for row in cursor.fetchall():
    filers.add(row[0])

bad_filer_ids = set()

for filer_id in filers:
    cursor = db.cursor()
    filer_data = {"filer_id": filer_id}

    cursor.execute("select election_year, filer_type_id, filer_name, office, jurisdiction, political_committee_type, position, url from registrations, registrations_filer_name where filer_name_id = registrations_filer_name.id AND filer_id = ? order by election_year desc limit 1", (filer_id,))
    cols = [x[0] for x in cursor.description]
    filer_data["info"] = [dict(zip(cols, x)) for x in cursor.fetchall()]
    if not filer_data["info"]:
        bad_filer_ids.add(filer_id)
        continue

    filer_data["info"] = filer_data["info"][0]

    cursor.execute("select contributor_name, code_id, SUM(amount) as total from contributions where filer_id = ? group by contributor_name order by total desc limit 10", (filer_id,))
    cols = [x[0] for x in cursor.description]
    filer_data["contributors"] = [dict(zip(cols, x)) for x in cursor.fetchall()]
    for row in filer_data["contributors"]:
        row["vip"] = row["contributor_name"] in contributors

    cursor.execute("select rowid, receipt_date, cash_or_in_kind_id, contributor_name, contributor_occupation, contributor_employer_name, amount, url, code_id from contributions where filer_id = ? order by receipt_date DESC limit 20", (filer_id,))
    cols = [x[0] for x in cursor.description]
    filer_data["contributions"] = [dict(zip(cols, x)) for x in cursor.fetchall()]
    for row in filer_data["contributions"]:
        row["vip"] = row["contributor_name"] in contributors

    cursor.execute("select rowid, expenditure_date, recipient_name, description, amount, url from expenditures where filer_id = ? order by expenditure_date DESC limit 20", (filer_id,))
    cols = [x[0] for x in cursor.description]
    filer_data["expenditures"] = [dict(zip(cols, x)) for x in cursor.fetchall()]

    filer_template = jinja_env.get_template("pages/filer/{filer_id}.html")

    out = pathlib.Path("site")
    filer_out = out / "filer" / filer_id / "index.html"
    filer_out.parent.mkdir(parents=True, exist_ok=True)
    filer_out.write_text(filer_template.render(**filer_data))

# Remove filer ids we couldn't render
filers -= bad_filer_ids

elections = [{"date": "20211102",
    "year": 2021,
    "title": "2021 November 2nd General Election",
    "jurisdictions" :{
    "King County": {
        "Executive": ("CONSJ  072", "NGUYJ  102"),
        "Council District No. 1": ("CAVES--117", "DEMBR  101"),
        "Council District No. 3": ("LAMBK  052", "PERRS--027"),
        "Council District No. 5": ("OLOWS--108", "UPTHD  198"),
        "Council District No. 7": ("VONRP  023", "TORGD--042"),
        "Council District No. 9": ("DUNNR  059", "VAN K  057"),
    },
    "City of Seattle": {
        "Mayor": ("HARRB  101", "BRHASE 188", "GONZM  102", "ESWFE--109"),
        "City Attorney": ("THOMN--101", "SATTA  115", "SENSS--851"),
        "Council Position No. 8": ("MOSQT  102", "WILSK--103"),
        "Council Position No. 9": ("OLIVN  102", "NELSS  104", "SEATC--021"),
    },
    "Port of Seattle": {
        "Commissioner Position No. 1": ("SIGLN  122", "CALKR  104"),
        "Commissioner Position No. 3": ("BOWMS  124", "MOHAH--168"),
        "Commissioner Position No. 4": ("STEIP  104", "HASET--104"),
    },
    "Seattle School District No. 1": {
        "Director District No. 4": ("MARIV--119", "RIVEL--134"),
        "Director District No. 5": ("SARJM--102", "HARDD  102"),
        "Director District No. 7": ("HERSB--890",),
    }
}}]

for year in range(2021, 2023):
    year_data = {"year": year, "elections": []}

    for election in elections:
        if election["year"] == year:
            year_data["elections"].append(election)

    cursor = db.cursor()

    cursor.execute("select type_id, filer_name, filer_id, sum(amount) as total from contributions, contributions_filer_name WHERE filer_name_id = contributions_filer_name.id and election_year = ? group by filer_id order by total DESC limit 20", (year,))
    cols = [x[0] for x in cursor.description]
    year_data["fundraisers"] = [dict(zip(cols, x)) for x in cursor.fetchall()]
    for row in year_data["fundraisers"]:
        row["vip"] = row["filer_id"] in filers

    cursor.execute("select contributor_name, code_id, total_amount from contribution_totals where election_year = ? order by total_amount desc limit 20", (year,))
    cols = [x[0] for x in cursor.description]
    year_data["contributors"] = [dict(zip(cols, x)) for x in cursor.fetchall()]
    for row in year_data["contributors"]:
        row["vip"] = row["contributor_name"] in contributors
    index_data.append(year_data)

index_template = jinja_env.get_template("pages/index.html")

out = pathlib.Path("site")
(out / "index.html").write_text(index_template.render(data=reversed(index_data)))

for contributor_name in contributors:
    cursor = db.cursor()
    contributor_data = {"contributor_name": contributor_name}

    cursor.execute("select type_id, filer_name, filer_id, sum(amount) as total from contributions, contributions_filer_name where filer_name_id = contributions_filer_name.id AND contributor_name = ? group by filer_id order by total DESC limit 10", (contributor_name,))
    cols = [x[0] for x in cursor.description]
    filer_data["top_filers"] = [dict(zip(cols, x)) for x in cursor.fetchall()]
    for row in filer_data["top_filers"]:
        row["vip"] = row["filer_id"] in filers

    cursor.execute("select strftime('%Y', receipt_date) as year, SUM(amount) as total from contributions where contributor_name = ? group by year order by year DESC limit 50", (contributor_name,))
    cols = [x[0] for x in cursor.description]
    filer_data["year_totals"] = [dict(zip(cols, x)) for x in cursor.fetchall()]

    cursor.execute("select receipt_date, type_id, filer_name, filer_id, amount from contributions, contributions_filer_name where filer_name_id = contributions_filer_name.id AND contributor_name = ? order by receipt_date DESC limit 20", (contributor_name,))
    cols = [x[0] for x in cursor.description]
    filer_data["recent_donations"] = [dict(zip(cols, x)) for x in cursor.fetchall()]
    for row in filer_data["recent_donations"]:
        row["vip"] = row["filer_id"] in filers

    filer_template = jinja_env.get_template("pages/donor/{donor_name}.html")

    out = pathlib.Path("site")
    filer_out = out / "donor" / contributor_name / "index.html"
    filer_out.parent.mkdir(parents=True, exist_ok=True)
    filer_out.write_text(filer_template.render(**filer_data))

election_template = jinja_env.get_template("pages/election/20211102.html")

for election in elections:
    cursor = db.cursor()
    for jurisdiction, positions in election["jurisdictions"].items():
        for position, filers in positions.items():
            new_filer_data = []
            for filer_id in filers:
                filer_data = {"filer_id": filer_id}
                cursor.execute("select filer_name, filer_id, filer_type_id from registrations, registrations_filer_name where filer_name_id = registrations_filer_name.id and election_year = 2021 and filer_id = ? LIMIT 1", (filer_id,))
                cols = [x[0] for x in cursor.description]
                filer_data["filer"] = [dict(zip(cols, x)) for x in cursor.fetchall()][0]

                cursor.execute("select sum(amount) as total from contributions where election_year = 2021 and filer_id = ?", (filer_id,))
                cols = [x[0] for x in cursor.description]
                filer_data["totals"] = [dict(zip(cols, x)) for x in cursor.fetchall()][0]

                cursor.execute("select count(distinct contributor_name) as count from contributions where election_year = 2021 and filer_id = ?", (filer_id,))
                cols = [x[0] for x in cursor.description]
                filer_data["contributors"] = [dict(zip(cols, x)) for x in cursor.fetchall()][0]
                new_filer_data.append(filer_data)
            positions[position] = new_filer_data


    out = pathlib.Path("site")
    election_out = out / "election" / election["date"] / "index.html"
    election_out.parent.mkdir(parents=True, exist_ok=True)
    election_out.write_text(election_template.render(**election))
