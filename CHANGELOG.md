# Changelog

## 0.0.6
  * Fix issue with sync for `conversations` and child `conversation_threads`. Change to FULL_TABLE replication.

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
