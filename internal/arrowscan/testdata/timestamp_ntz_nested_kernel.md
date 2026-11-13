Captured from a live test warehouse on 2026-11-13 through the kernel backend (bindings v1.4.0)
with session `timezone=America/New_York`. The `.arrow` file is an Arrow IPC stream holding the
first result batch exactly as Go imports it from the kernel's C Data export:

```sql
SELECT
  array(TIMESTAMP_NTZ '2024-06-15 09:15:00.123456') AS ntz_array,
  map('a', TIMESTAMP_NTZ '2024-06-15 09:15:00.123456') AS ntz_map,
  named_struct('label', 'pt',
               'ntz', TIMESTAMP_NTZ '2024-06-15 09:15:00.123456',
               'zoned', TIMESTAMP '2024-06-15 09:15:00.123456',
               'inner', named_struct('ntz', TIMESTAMP_NTZ '2024-06-15 09:15:00.123456')) AS ts_struct,
  map(TIMESTAMP_NTZ '2024-06-15 09:15:00.123456', 1) AS ntz_key_map,
  array(named_struct('ntz', TIMESTAMP_NTZ '2024-06-15 09:15:00.123456',
                     'zoned', TIMESTAMP '2024-06-15 09:15:00.123456')) AS struct_array
```

Nested NTZ children arrive as `timestamp[us, tz=UTC]` with no field metadata; only the
top-level fields carry `databricks.type_name` and `Spark:DataType:JsonType`.
