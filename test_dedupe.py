#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This is an example of working with very large data. There are about
700,000 unduplicated donors in this database of Illinois political
campaign contributions.

With such a large set of input data, we cannot store all the comparisons
we need to make in memory. Instead, we will read the pairs on demand
from the MySQL database.

__Note:__ You will need to run `python mysql_init_db.py`
before running this script. See the annotates source for
[mysql_init_db.py](mysql_init_db.html)

For smaller datasets (<10,000), see our
[csv_example](csv_example.html)
"""

import os
import itertools
import time
import logging
import locale
import json

import sqlite3

import dedupe
import dedupe.backport

def record_pairs(result_set):
    for i, row in enumerate(result_set):
        a_record_id, a_record, b_record_id, b_record = row
        record_a = (a_record_id, json.loads(a_record))
        record_b = (b_record_id, json.loads(b_record))

        yield record_a, record_b

        if i % 10000 == 0:
            print(i)


def cluster_ids(clustered_dupes):

    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for donor_id, score in zip(cluster, scores):
            yield donor_id, cluster_id, score

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added
# for convenience.  To enable verbose output, run `python
# examples/mysql_example/mysql_example.py -v`
# logging.WARNING
#  logging.INFO
logging.getLogger().setLevel(logging.DEBUG)

settings_file = 'clean_settings'
training_file = 'raw_training.json'

start_time = time.time()

con = sqlite3.connect("temp.db")
con.row_factory = sqlite3.Row

# ## Training

class RowIter:
    def __init__(self, cur):
        self._cur = cur

    def __iter__(self):
        return self

    def __next__(self):
        r = self._cur.fetchone()
        if r is None:
            print("done")
            raise StopIteration()
        return dict(r)

class SQLDict:
    def __init__(self, connection):
        self._con = connection

    def __len__(self):
        cur = self._con.cursor()
        cur.execute("SELECT COUNT(*) FROM contributions  WHERE contributor_zip = 98107 GROUP BY contributor_category_id, contributor_name, contributor_address")
        count = cur.fetchone()[0]
        print("count", count)
        return count

    def values(self):
        cur = self._con.cursor()
        cur.execute("SELECT rowid, contributor_category_id, contributor_name, contributor_address FROM contributions WHERE contributor_zip = 98107 GROUP BY contributor_category_id, contributor_name, contributor_address")
        return RowIter(cur)

    def __iter__(self):
        return self.values()


if os.path.exists(settings_file):
    print('reading from ', settings_file)
    with open(settings_file, 'rb') as sf:
        deduper = dedupe.StaticDedupe(sf, num_cores=4)
else:
    # Define the fields dedupe will pay attention to
    #
    # The address, city, and zip fields are often missing, so we'll
    # tell dedupe that, and we'll teach a model that take that into
    # account
    fields = [{'field': 'contributor_name', 'type': 'Name'},
              {'field': 'contributor_address', 'type': 'Address', 'has missing': True},
              # {'field': 'contributor_city', 'type': 'ShortString', 'has missing': True},
              # {'field': 'contributor_state', 'type': 'ShortString', 'has missing': True},
              # {'field': 'contributor_occupation', 'type': 'ShortString', 'has missing': True},
              # {'field': 'contributor_employer_name', 'type': 'ShortString', 'has missing': True},
            ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields, num_cores=4)

    # We will sample pairs from the entire donor table for training
    s = SQLDict(con)

    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    #
    # __Note:__ if you want to train from
    # scratch, delete the training_file
    print("prepping training data")
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file) as tf:
            deduper.prepare_training(s, training_file=tf)
    else:
        deduper.prepare_training(s)

    # ## Active learning

    print('starting active labeling...')
    # Starts the training loop. Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    dedupe.convenience.console_label(deduper)
    # When finished, save our labeled, training pairs to disk
    with open(training_file, 'w') as tf:
        deduper.write_training(tf)

    # Notice our the argument here
    #
    # `recall` is the proportion of true dupes pairs that the learned
    # rules must cover. You may want to reduce this if your are making
    # too many blocks and too many comparisons.
    deduper.train(recall=0.90)

    with open(settings_file, 'wb') as sf:
        deduper.write_settings(sf)

    # We can now remove some of the memory hobbing objects we used
    # for training
    deduper.cleanup_training()

# ## Blocking

print('blocking...')

# To run blocking on such a large set of data, we create a separate table
# that contains blocking keys and record ids
print('creating blocking_map database')
cur = con.cursor()
cur.execute("DROP TABLE IF EXISTS blocking_map")
cur.execute("CREATE TABLE blocking_map "
            "(block_key VARCHAR(200), rowid INTEGER)")

con.commit()

# If dedupe learned a Index Predicate, we have to take a pass
# through the data and create indices.
print('creating inverted index')

for field in deduper.fingerprinter.index_fields:
    cur = con.cursor()
    cur.execute("SELECT DISTINCT {field} FROM contributions "
                "WHERE {field} IS NOT NULL AND contributor_zip = 98107".format(field=field))
    field_data = (row[0] for row in cur)
    deduper.fingerprinter.index(field_data, field)

# Now we are ready to write our blocking map table by creating a
# generator that yields unique `(block_key, donor_id)` tuples.
print('writing blocking map')

read_cur = con.cursor()
read_cur.execute("SELECT rowid, contributor_category_id, contributor_name, contributor_address FROM contributions WHERE contributor_zip = 98107 GROUP BY contributor_category_id, contributor_name, contributor_address")
full_data = ((row['rowid'], row) for row in read_cur)
b_data = deduper.fingerprinter(full_data)

write_cur = con.cursor()
write_cur.executemany("INSERT INTO blocking_map VALUES (?, ?)",
                      b_data)
con.commit()

# Free up memory by removing indices we don't need anymore
deduper.fingerprinter.reset_indices()

# indexing blocking_map
print('creating index')
cur = con.cursor()
cur.execute("CREATE UNIQUE INDEX bm_idx ON blocking_map (block_key, rowid)")
con.commit()

# select unique pairs to compare
read_cur = con.cursor()
read_cur.execute("""
       select a.rowid,
              json_object('contributor_name', a.contributor_name,
                          'contributor_address', a.contributor_address),
              b.rowid,
              json_object('contributor_name', b.contributor_name,
                          'contributor_address', b.contributor_address)
       from (select DISTINCT l.rowid as east, r.rowid as west
             from blocking_map as l
             INNER JOIN blocking_map as r
             using (block_key)
             where l.rowid < r.rowid) ids
       INNER JOIN contributions a on ids.east=a.rowid
       INNER JOIN contributions b on ids.west=b.rowid
       """)

# ## Clustering

print('clustering...')
clustered_dupes = deduper.cluster(deduper.score(record_pairs(read_cur)),
                                  threshold=0.5)

write_cur = con.cursor()

# ## Writing out results

# We now have a sequence of tuples of donor ids that dedupe believes
# all refer to the same entity. We write this out onto an entity map
# table
write_cur.execute("DROP TABLE IF EXISTS entity_map")

print('creating entity_map database')
write_cur.execute("CREATE TABLE entity_map "
                  "(donor_id INTEGER, canon_id INTEGER, "
                  " cluster_score REAL, PRIMARY KEY(donor_id))")

for cluster in cluster_ids(clustered_dupes):
    print(cluster)
    write_cur.executemany('INSERT INTO entity_map VALUES (?, ?, ?)',
                          cluster)

con.commit()

cur = con.cursor()
cur.execute("CREATE INDEX head_index ON entity_map (canon_id)")
con.commit()

# Print out the number of duplicates found
print('# duplicate sets')

# ## Payoff

# With all this done, we can now begin to ask interesting questions
# of the data
#
# For example, let's see who the top 10 donors are.

locale.setlocale(locale.LC_ALL, '')  # for pretty printing numbers

with read_con.cursor() as cur:
    # Create a temporary table so each group and unmatched record has
    # a unique id
    cur.execute("CREATE TEMPORARY TABLE e_map "
                "SELECT IFNULL(canon_id, donor_id) AS canon_id, donor_id "
                "FROM entity_map "
                "RIGHT JOIN donors USING(donor_id)")

    cur.execute("SELECT CONCAT_WS(' ', donors.first_name, donors.last_name) AS name, "
                "donation_totals.totals AS totals "
                "FROM donors INNER JOIN "
                "(SELECT canon_id, SUM(amount) AS totals "
                " FROM contributions INNER JOIN e_map "
                " USING (donor_id) "
                " GROUP BY (canon_id) "
                " ORDER BY totals "
                " DESC LIMIT 10) "
                "AS donation_totals "
                "WHERE donors.donor_id = donation_totals.canon_id")

    print("Top Donors (deduped)")
    for row in cur:
        row['totals'] = locale.currency(row['totals'], grouping=True)
        print('%(totals)20s: %(name)s' % row)

    # Compare this to what we would have gotten if we hadn't done any
    # deduplication
    cur.execute("SELECT CONCAT_WS(' ', donors.first_name, donors.last_name) as name, "
                "SUM(contributions.amount) AS totals "
                "FROM donors INNER JOIN contributions "
                "USING (donor_id) "
                "GROUP BY (donor_id) "
                "ORDER BY totals DESC "
                "LIMIT 10")

    print("Top Donors (raw)")
    for row in cur:
        row['totals'] = locale.currency(row['totals'], grouping=True)
        print('%(totals)20s: %(name)s' % row)

    # Close our database connection
read_con.close()
write_con.close()

print('ran in', time.time() - start_time, 'seconds')
