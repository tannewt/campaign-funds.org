import pandas
import sqlite3
import recordlinkage
import time
import pathlib
con = sqlite3.connect("file:/home/tannewt/repos/campaign-funds.org/raw.db?mode=ro", uri=True)
df = pandas.read_sql_query("SELECT rowid, contributor_name, contributor_address, count(*) as num FROM contributions WHERE contributor_zip = 98107 GROUP BY contributor_name, contributor_address ORDER BY num DESC", con, index_col="rowid")
print(df)

indexer = recordlinkage.Index()
indexer.full()
candidates = indexer.index(df)

print(candidates)

compare = recordlinkage.Compare()
compare.string("contributor_name", "contributor_name", method="levenshtein", label="contributor_name_lev")
compare.string("contributor_name", "contributor_name", method="jarowinkler", label="contributor_name_jw")
compare.string("contributor_address", "contributor_address", method="levenshtein", label="contributor_address_lev")
# compare.string("contributor_state", "contributor_state", label="contributor_state")
# compare.string("contributor_city", "contributor_city", label="contributor_city")
# compare.exact("contributor_zip", "contributor_zip", label="contributor_zip", missing_value=1)

stored = pathlib.Path("compare_numbers.feather")
start = time.monotonic()
if stored.exists():
    print("loading cached comparison numbers")
    features = pandas.read_feather("compare_numbers.feather", columns=["rowid_1", "rowid_2", "contributor_name_lev", "contributor_name_jw", "contributor_address_lev"])
    print(features)
    features.set_index(["rowid_1", "rowid_2"], inplace=True)
    print(features)
else:
    print("comparing")
    features = compare.compute(candidates, df)
duration = time.monotonic() - start

features.reset_index().to_feather("compare_numbers.feather")

golden_data = pathlib.Path("golden_data").iterdir()
if not golden_data:
    connected = recordlinkage.ConnectedComponents()
    matches = features[features.sum(axis=1) > 2.5]
    print("writing annotation file")
    # TODO: write out in chunks
    recordlinkage.write_annotation_file("annotations_todo.json",
        candidates[:200],
        df)
    print(matches[0:1])
    groups = connected.compute(matches.index)
else:
    print("train!")
    classifier = recordlinkage.NaiveBayesClassifier()
    sample = None
    matches = None
    for path in golden_data:
        print("loading", path)
        results = recordlinkage.read_annotation_file(path)
        if sample is None:
            matches = results.links
            sample = results.distinct
            sample = pandas.concat(results.distinct, results.links)
            print(sample)
        # else:
        #     matches.append(results.links)
        #     sample.append(results.distinct)
    print(sample)
    print(features.loc[sample])
    # classifier.fit(features.loc[sample], matches)
    # r = classifier.predict(features)
    # print(r)

matched = set()
for g in groups:
    f = g.to_frame()
    s = set(f[0].unique())
    s.update(f[1].unique())
    print(s)
    print(df.loc[list(s)])
    print()
    matched.update(s)

print(len(groups), "groups from", len(matched), "matched records")
unmatched = set(df.index) - matched
print(len(unmatched), "unmatched")
print("comparison time", duration)

# for unmatched in set(df.index) - matched:
#     print(unmatched)
#     print(df.loc[unmatched])
#     print()
