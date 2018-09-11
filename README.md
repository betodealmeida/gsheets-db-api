# A Python DB API 2.0 for Google Spreadsheets #

Using [this spreadsheet](https://docs.google.com/spreadsheets/d/1q9REzifHb90vewm4XMjnWFKOPNTcG6Xh8s6Hwo9OpFo/edit?usp=sharing) as an example:

```python
>>> from gsheetsdb import connect
>>> conn = connect('https://docs.google.com/spreadsheets/d/1q9REzifHb90vewm4XMjnWFKOPNTcG6Xh8s6Hwo9OpFo/', headers=2)
>>> result = conn.execute('SELECT A, SUM(B) GROUP BY A')
>>> for row in result:
...     print(row)
...
Row(_0='BR', _1=4.0)
Row(_0='IN', _1=5.0)
>>> print(result.description)
[('country string', <Type.STRING: 1>, None, None, None, None, True), ('sum cnt number', <Type.NUMBER: 2>, None, None, None, None, True)]
```
