# Changelog

## Current (in progress)

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
