[![Build Status](https://travis-ci.org/betodealmeida/gsheets-db-api.svg?branch=master)](https://travis-ci.org/betodealmeida/gsheets-db-api) [![codecov](https://codecov.io/gh/betodealmeida/gsheets-db-api/branch/master/graph/badge.svg)](https://codecov.io/gh/betodealmeida/gsheets-db-api)



# A Python DB API 2.0 for Google Spreadsheets #

This module allows you to query Google Spreadsheets using SQL.

Using [this spreadsheet](https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/) as an example:

| | A | B |
|-|--------|-----|
| 1 | country | cnt |
| 2 | BR | 1 |
| 3 | BR | 3 |
| 4 | IN | 5 |

Here's a simple query using the Python API:

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
$ pip install gsheetsdb[cli]         # if you want to use the CLI
$ pip install gsheetsdb[sqlalchemy]  # if you want to use it with SQLAlchemy
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

## SQLAlchemy support ##

This module provides a SQLAlchemy dialect. You don't need to specify a URL, since the spreadsheet is extracted from the `FROM` clause:

```python
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *

engine = create_engine('gsheets://')
inspector = inspect(engine)

table = Table(
    'https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0',
    MetaData(bind=engine),
    autoload=True)
query = select([func.count(table.columns.country)], from_obj=table)
print(query.scalar())  # prints 3.0
```

Alternatively, you can initialize the engine with a "catalog". The catalog is a Google spreadsheet where each row points to another Google spreadsheet, with URL, number of headers and schema as the columns. You can see an example [here](https://docs.google.com/spreadsheets/d/1AAqVVSpGeyRZyrr4n--fb_IxhLwwKtLbjfu4h6MyyYA/edit#gid=0):

|| A | B | C |
|-|-|-|-|
| 1 | https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=0 | 1 | default |
| 2 | https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/edit#gid=1077884006 | 2 | default |

This will make the two spreadsheets above available as "tables" in the `default` schema.
