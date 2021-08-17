import sqlite3
con = sqlite3.connect('contributions.db')
con.execute("DROP TABLE contribution_totals")
con.execute("CREATE TABLE contribution_totals AS SELECT contributor_name, election_year, SUM(amount) as total_amount FROM Contributions_to_Candidates_and_Political_Committees GROUP BY contributor_name, election_year")
con.commit()
