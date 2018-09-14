# A Python DB API 2.0 for Google Spreadsheets #

Using [this spreadsheet](https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/) as an example:

```python
from gsheetsdb import connect
conn = connect()
result = conn.execute("""
    SELECT
        country
      , SUM(cnt)
    FROM
        "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/"
    GROUP BY
        country
""", headers=1)
for row in result:
    print(row)
```

This will print:

```
Row(country='BR', sum_cnt=4.0)
Row(country='IN', sum_cnt=5.0)
```

## Installation ##

```bash
$ pip install gsheetsdb
$ pip install gsheetsdb[cli]  # if you want to use the CLI; see below
```

## CLI ##

The module will install an executable called `gsheetsdb`:

```bash
$ gsheetsdb --headers=1
> SELECT * FROM "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/"
country      cnt
---------  -----
BR             1
BR             3
IN             5
> SELECT country, SUM(cnt) FROM "https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1
pscv8ZXPtg8/" GROUP BY country
country      sum cnt
---------  ---------
BR                 4
IN                 5
>
```
