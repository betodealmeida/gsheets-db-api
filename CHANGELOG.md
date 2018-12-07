# Changelog

## [0.1.0] - 2018-09-16
- Initial release providing a Python DB API SQL interface to Google spreadsheets and a CLI.

## [0.1.1] - 2018-09-16
- Added SQLAlchemy dialect.
- Allow headers and gid to be passed on the URL.

## [0.1.2] - 2018-09-16
- Add missing dependency to `moz-sql-parser` to `setup.py`.

## [0.1.3] - 2018-09-16
- Fix small bug in SQL Alchemy compiler.
- Allow aliases in `ORDER BY`.

## [0.1.4] - 2018-09-16
- Fix `visit_column` method.

## [0.1.5] - 2018-10-25
- Parse dates, better error message.
- `COUNT(*)` working.
- Fallback to SQLite if query fails.
- Custom date truncation using `DATETRUNC`.

## [0.1.6] - 2018-10-30
- Handle authentication.
- Fix cursor description in SQLite fallback.

## [0.1.7] - 2018-12-06
- Fix session when no credentials are passed.

## [0.1.8] - 2018-12-07
- Add logging.
- Fix `CREATE TABLE` when sheet has no headers.
