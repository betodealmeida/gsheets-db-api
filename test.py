from gsheetsdb import connect

c = connect('https://docs.google.com/spreadsheets/d/1q9REzifHb90vewm4XMjnWFKOPNTcG6Xh8s6Hwo9OpFo/', headers=2)
r = c.execute('SELECT A, SUM(B) GROUP BY A')
for row in r:
    print(row)
print(r.description)
