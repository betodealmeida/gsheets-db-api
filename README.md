# A Python DB API 2.0 for Google Spreadsheets #

Using [this spreadsheet](https://docs.google.com/spreadsheets/d/1_rN3lm0R_bU3NemO0s9pbFkY5LQPcuy1pscv8ZXPtg8/) as an example:

```python
>>> from gsheetsdb import connect
>>> conn = connect('https://docs.google.com/spreadsheets/d/1q9REzifHb90vewm4XMjnWFKOPNTcG6Xh8s6Hwo9OpFo/', headers=1)
>>> result = conn.execute('SELECT A, SUM(B) GROUP BY A')
>>> for row in result:
...     print(row)
...
Row(country='BR', sum_cnt=4.0)
Row(country='IN', sum_cnt=5.0)
>>> print(result.description)
[('country', <Type.STRING: 1>, None, None, None, None, True), ('sum cnt', <Type.NUMBER: 2>, None, None, None, None, True)]
```
