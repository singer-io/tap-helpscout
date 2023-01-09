# Changelog
## 1.1.0
  * Adds support for dev mode [#30](https://github.com/singer-io/tap-helpscout/pull/30)

## 1.0.3
  * Change `conversations` to use bookmark `modifiedSince` filter param to improve bookmarking and reduce the data volumes queried.

## 1.0.2
  * Get max `updated_at` for `conversations` even if `None`.

## 1.0.1
  * Add `updated_at` as bookmark for `conversations`. This is a new bookmark field, based on max of 2 other datetimes in record.

## 1.0.0
  * Releasing from Beta --> GA

## 0.0.5
  * Update `README.md` documentation for each endpoint

## 0.0.4
  * Adjust `sync.py` to simplify and fix issue with missing child `conversation_threads`
  * Update `README.md` documentation

## 0.0.3
  * Fix discovery and sync when child nodes are de-selected

## 0.0.2
  * Remove `schema_name` parameter from `get_standard_metadata` as this should only be used by databases with hierarchical schemas

## 0.0.1
  * Initial commit
