# Blob CSV ingestion – pipeline wiring guide

## What changed

The pipeline already ingested from Azure SQL (`source_type = 'sql'`).
A **Switch** activity inside the ForEach now routes by `source_type`,
so it also handles `source_type = 'blob'` (CSV files in Azure Blob Storage).

## Pipeline layout

```
Lookup (meta.pipeline_config WHERE enabled = 1)
  └── ForEach (items = @activity('LookupConfig').output.value)
        └── Switch (on = @item().source_type)
              ├── Case 'sql'  → Copy_sql_to_bronze → Validate_count
              ├── Case 'blob' → Copy_blob_to_bronze
              └── Default     → (empty; add Fail later if needed)
```

### Fabric nesting constraint
Fabric does not allow control-flow activities (If, Switch, ForEach)
inside a Switch case or inside another If branch. The count check
therefore uses a single Lookup with a THROW query instead of the
original Lookup → If → Fail chain.

### Count check for blob rows
Skipped by design — the CSV is small and static, and there is no SQL
source table to count. A stronger check (count the bronze table via
the SQL endpoint) can be added later.

## Key expressions

### LookupConfig query
```
SELECT source_type, source_schema, table_name, target_table
FROM meta.pipeline_config WHERE enabled = 1 ORDER BY load_order
```

### Switch expression
```
@item().source_type
```

### Copy_sql_to_bronze – Source query
```
@concat('SELECT * FROM [', item().source_schema, '].[', item().table_name, ']')
```

### Copy_sql_to_bronze – Destination
- Lakehouse: `lh_data`
- Table: `@item().target_table`
- Table action: Overwrite
- Mapping: empty

### Validate_count – query (inside sql case, after Copy_sql_to_bronze)
```
@concat('IF (SELECT COUNT_BIG(*) FROM [', item().source_schema, '].[', item().table_name, ']) <> ', activity('Copy_sql_to_bronze').output.rowsCopied, ' THROW 50001, ''Count mismatch'', 1; SELECT 1 AS ok;')
```
If counts match → Lookup succeeds. If they don't → SQL throws, Lookup fails, pipeline stops.

### Copy_blob_to_bronze – Source
- Connection: Azure Blob Storage
- Container: `@item().source_schema`
- File name: `@item().table_name`
- File format: DelimitedText, first row as header
- Directory: empty

### Copy_blob_to_bronze – Destination
- Lakehouse: `lh_data`
- Table: `@item().target_table`
- Table action: Overwrite
- Mapping: empty

## Config row for the CSV

```sql
INSERT INTO meta.pipeline_config
    (id, source_type, source_schema, table_name, target_table, load_order, description)
VALUES
    (8, 'blob', 'blob-landing-files', 'IWInsurance_peril_code_map.csv',
     'peril_code_map', 80, 'Peril code lookup from blob CSV');
```

## Gotchas found during wiring

1. **LookupConfig must SELECT source_type** — the original query only returned
   three columns; the Switch expression failed with "property doesn't exist".
2. **File name = table_name, not target_table** — using `target_table` gave a
   404 because it resolved to the bare name without the `.csv` extension.
3. **Fabric nesting limits** — you cannot place If, Switch or ForEach inside a
   Switch case or inside another If branch. Use a THROW-based Lookup for
   conditional validation instead.
4. **File format must be DelimitedText** — the default Binary won't parse CSV
   headers or write columnar data to the lakehouse.
