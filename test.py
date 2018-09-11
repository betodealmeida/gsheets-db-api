from gsheetsdb import connect

c = connect('https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/', headers=1)
r = c.execute('SELECT A, SUM(B) GROUP BY A')
for row in r:
    print(row)
print(r.description)
