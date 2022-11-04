# Changelog

## Current (in progress)

- Nothing yet

## 2.2.0 (2022-11-04)

- Remove profile endpoint, disable thread pool for profiling [#135](https://github.com/etalab/csvapi/pull/135)
- Fix tests by using a custom docker image [#135](https://github.com/etalab/csvapi/pull/135)

## 2.1.1 (2022-10-25)

* Fix bugs [#126](https://github.com/etalab/csvapi/pull/126) with json files

## 2.1.0 (2022-10-13)

* Fix bugs [#110](https://github.com/etalab/csvapi/pull/110) and [#111](https://github.com/etalab/csvapi/pull/111)
* Add endpoint API greater_than or less_than int or float value [#109](https://github.com/etalab/csvapi/pull/109)
* Update version csv-detective [#119](https://github.com/etalab/csvapi/pull/119)

## 2.0.0 (2022-09-15)

- [BREAKING] Migrate to python >= 3.9 [#104](https://github.com/etalab/csvapi/pull/104)
- Migrate to poetry [#104](https://github.com/etalab/csvapi/pull/104)
- Enrich sqlite dbs with metadata extracted from csv-detective and pandas profiling [#104](https://github.com/etalab/csvapi/pull/104)
- Enrich apify api with possibility to analyse resource [#104](https://github.com/etalab/csvapi/pull/104)

## 1.2.1 (2021-04-29)

- Upgrade raven to sentry-sdk (a bit dirty so far)

## 1.2.0 (2021-04-29)

- Add profiling support [#77](https://github.com/etalab/csvapi/pull/77)
- Fix bug in filters w/ blanks in column names [#77](https://github.com/etalab/csvapi/pull/77)

## 1.1.0 (2021-03-23)

- Use aiosqlite [#76](https://github.com/etalab/csvapi/pull/76)

## 1.0.6 (2020-12-14)

- Better parsing fallback [#71](https://github.com/etalab/csvapi/pull/71)

## 1.0.5 (2020-11-17)

- Parsing view now raises exception on http error response codes [#69](https://github.com/etalab/csvapi/pull/69)

## 1.0.4 (2020-10-26)

- Protect custom type testers against None values [#66](https://github.com/etalab/csvapi/pull/66)
- Fix xlsx file support [#67](https://github.com/etalab/csvapi/pull/67)

## 1.0.3 (2020-03-04)

- Fix packaging problem

## 1.0.2 (2020-03-04)

- Fix XLS parsing [#60](https://github.com/etalab/csvapi/pull/60)

## 1.0.1 (2020-01-03)

- Fix aiohttp import [#52](https://github.com/etalab/csvapi/pull/52)

## 1.0.0 (2020-01-03)

- Add filters support [#50](https://github.com/etalab/csvapi/pull/50)
- Replace requests by aiohttp for asynchronous http requests. Also replace every format() string to use only f"strings. [#46](https://github.com/etalab/csvapi/pull/46)

## 0.1.0 (2019-09-06)

- Upgrade to Quart-0.9.1 :warning: requires python-3.7 [#21](https://github.com/opendatateam/csvapi/pull/21)
- Parse hours, SIREN and SIRET as text [#42](https://github.com/opendatateam/csvapi/pull/42)

## 0.0.9 (2019-01-18)

- Upgrade to Quart-0.6.6 and hypercorn-0.4.6 [#16](https://github.com/opendatateam/csvapi/pull/16)

## 0.0.8 (2018-10-04)

- Try to parse CSV w/o sniffing (excel dialect) after sniffing if it fails

## 0.0.7 (2018-09-17)

- `MAX_FILE_SIZE` config variable [#13](https://github.com/opendatateam/csvapi/pull/13)
- Add filter by referrer feature (REFERRERS_FILTER) [#14](https://github.com/opendatateam/csvapi/pull/14)

## 0.0.6 (2018-09-10)

- Compute the total number of rows in a table [#12](https://github.com/opendatateam/csvapi/pull/12)

## 0.0.5 (2018-09-10)

- Make CSV sniff limit a config variable and raise the default value [#11](https://github.com/opendatateam/csvapi/pull/11)
- Properly handle not found (404) errors

## 0.0.4 (2018-09-04)

- FORCE_SSL config variable

## 0.0.3 (2018-08-31)

- Sentry support via SENTRY_DSN config variable

## 0.0.2 (2018-08-30)

- CSVAPI_CONFIG_FILE env var support

## 0.0.1 (2018-08-30)

- Initial version
