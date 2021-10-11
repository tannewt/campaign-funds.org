import pandas
import sqlite3
import recordlinkage
import time
import pathlib
con = sqlite3.connect("file:/home/tannewt/repos/campaign-funds.org/raw.db?mode=ro", uri=True)
contributions = pandas.read_sql_query("SELECT contributions.rowid, contributions.id, origin_id, amount, contributions.receipt_date, filer_name, committee_address, contributor_name, contributor_address FROM contributions LEFT JOIN (SELECT committee_id, filer_name_id, committee_address FROM registrations GROUP BY committee_id) as registrations ON registrations.committee_id = contributions.committee_id, registrations_filer_name WHERE registrations_filer_name.id = registrations.filer_name_id AND code_id >= 4 AND code_id <= 7 AND contributions.receipt_date IS NOT NULL AND amount > 0 ORDER BY contributions.receipt_date", con, index_col="rowid")
print(contributions)
expenditures = pandas.read_sql_query("SELECT expenditures.rowid, expenditures.id, origin_id, amount, expenditure_date, filer_name, committee_address, recipient_name, recipient_address FROM expenditures LEFT JOIN (SELECT committee_id, filer_name_id, committee_address FROM registrations GROUP BY committee_id) as registrations ON expenditures.committee_id = registrations.committee_id, registrations_filer_name WHERE registrations_filer_name.id = registrations.filer_name_id AND amount > 0 AND recipient_address IS NOT NULL ORDER BY expenditure_date", con, index_col="rowid")
print(expenditures)
print("contribution count", len(contributions))

indexer = recordlinkage.Index()
indexer.sortedneighbourhood(left_on="receipt_date", right_on="expenditure_date", window=3, block_on="amount")
potential_matches = indexer.index(contributions, expenditures)

print(len(potential_matches), "potential matches")

compare = recordlinkage.Compare()
# compare.string("contributor_name", "contributor_name", method="levenshtein", label="contributor_name_lev")
compare.string("contributor_name", "filer_name", method="jarowinkler", label="contributor_name_jw")
compare.string("filer_name", "recipient_name", method="jarowinkler", label="recipient_name_jw")
compare.string("contributor_address", "committee_address", method="levenshtein", label="contributor_address_lv")
compare.string("committee_address", "recipient_address", method="levenshtein", label="recipient_address_lv")

stored = pathlib.Path("match_numbers.feather")
start = time.monotonic()
if stored.exists():
    print("loading cached comparison numbers")
    features = pandas.read_feather("match_numbers.feather", columns=["rowid_1", "rowid_2", "contributor_name_jw", "recipient_name_jw", "contributor_address_lv", "recipient_address_lv"])
    features.set_index(["rowid_1", "rowid_2"], inplace=True)
else:
    print("comparing")
    features = compare.compute(potential_matches, contributions, expenditures)
    features.reset_index().to_feather("match_numbers.feather")
duration = time.monotonic() - start
print("done in", duration)

features = features.join(contributions, on=("rowid_1"), how="inner").join(expenditures, on=("rowid_2"), how="inner", lsuffix="_cont", rsuffix="_exp")
features["score"] = features["contributor_name_jw"] * features["recipient_name_jw"] * features["recipient_address_lv"] * features["contributor_address_lv"]
features = features[features["score"] > 0.2]
# features.sort_values("score", inplace=True)
features.sort_index(level=0, inplace=True)
maxes = features.groupby(level=0)["score"].transform(max) == features["score"]
features[maxes].to_csv("matched_contributions.csv")
print(features[maxes])

# # for unmatched in set(df.index) - matched:
# #     print(unmatched)
# #     print(df.loc[unmatched])
# #     print()
