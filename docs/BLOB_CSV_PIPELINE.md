# Blob CSV ingestion – pipeline wiring guide

## What changes

The pipeline already ingests from Azure SQL (`source_type = 'sql'`).
We add a **Switch** activity inside the ForEach so it also handles
`source_type = 'blob'` (CSV files in Azure Blob Storage).

After the Switch, count check stays the same for both paths.

## Pipeline layout after this change

```
Lookup (meta.pipeline_config WHERE enabled = 1)
  └── ForEach (items = @activity('Lookup').output.value)
        └── Switch (on = @item().source_type)
              ├── Case 'sql'  → Copy_sql_to_bronze (existing Copy activity, renamed)
              ├── Case 'blob' → Copy_blob_to_bronze (new Copy activity)
              └── Default     → Fail ("Unknown source_type")
        └── Count_source (Lookup – same as before, but only for sql rows)
        └── Check_counts (If – same as before)
        └── Fail (in the False branch of Check_counts)
```

> **Note**: Count check for blob rows uses a different approach because
> you cannot run `COUNT_BIG(*)` against a CSV file. Options:
> 1. Skip count check for blob rows (simplest; the file is small and static).
> 2. After Copy, count the bronze table via the SQL endpoint.
> We start with option 1. An If activity before Count_source skips it
> when `@equals(item().source_type, 'blob')`.

## Step-by-step in Fabric

### 1. Insert config row 8

Run this in `sqldb_control`:

```sql
INSERT INTO meta.pipeline_config
    (id, source_type, source_schema, table_name, target_table, load_order, description)
VALUES
    (8, 'blob', 'blob-landing-files', 'IWInsurance_peril_code_map.csv', 'peril_code_map', 80, 'Peril code lookup from blob CSV');
```

### 2. Create a connection to blob storage

In Fabric workspace → **Manage connections and gateways**:
- Connection type: **Azure Blob Storage**
- Server: your storage account URL (`https://<account>.blob.core.windows.net`)
- Authentication: **Organizational account** (dev) or **Service Principal** (shared)

### 3. Add the Switch activity

1. Open `pl_ingest_sources` pipeline.
2. Inside the ForEach, **delete the direct link** from ForEach → Copy_to_bronze.
3. Add a **Switch** activity.
4. Expression (via Add dynamic content): `@item().source_type`
5. Add case `sql`. Move the existing Copy_to_bronze into it (rename to `Copy_sql_to_bronze`).
6. Add case `blob`. Add a new **Copy data** activity named `Copy_blob_to_bronze`.
7. Add a **Default** case with a **Fail** activity: message `Unknown source_type: @{item().source_type}`, error code `UNKNOWN_SOURCE_TYPE`.

### 4. Configure Copy_blob_to_bronze

**Source**:
- Connection: the blob connection from step 2
- File path (via dynamic content):
  - Container: `@item().source_schema`
  - File name: `@item().table_name`
- File format: **DelimitedText** (CSV)
- First row as header: **Yes**

**Destination**:
- Connection: `lh_data` (same lakehouse)
- Table: via dynamic content → `@item().target_table`
- Table action: **Overwrite**
- Leave Mapping empty (same pattern as SQL copies)

### 5. Wire count check for sql-only rows

Add an **If condition** activity named `Is_sql_source` after the Switch:
- Expression: `@equals(item().source_type, 'sql')`
- **True** branch: move Count_source → Check_counts → Fail into it
- **False** branch: empty (blob rows skip count check for now)

### 6. Test

1. Run the pipeline. All 7 SQL tables + 1 CSV should succeed.
2. Check `bronze.peril_code_map` exists in lh_data with the CSV data.
3. Verify the 7 SQL tables still pass count check.
