# Medical Coding ETL Example

This example demonstrates how to use the ETL system for processing medical coding data from CSV files into multiple normalized PostgreSQL tables with transaction support, deduplication, and notifications.

## Features

- **Config-Driven**: Everything is configured via YAML
- **Multiple Table Mapping**: Single CSV source splits into multiple destination tables
- **Transaction Support**: Built-in transaction handling with rollback
- **Deduplication**: Prevents duplicate processing using encounter_id
- **Filtering**: Only inserts records when relevant fields are present
- **Notifications**: Email and webhook notifications on success/failure
- **Resumable**: State management for recovery from failures
- **Checkpoint Support**: Resume from last processed record

## Architecture

The system uses the existing ETL framework without custom loaders:
- `Manager.Prepare()` creates multiple ETL jobs from the config
- Each table mapping is processed as a separate ETL job
- Filter transformers skip records that don't have relevant data
- Built-in sqladapter handles database operations with transactions
- Deduplication ensures no duplicate encounters are processed

## Configuration

See `medical_coding_config.yaml` for the complete configuration including:

1. **Source**: CSV file configuration
2. **Destination**: PostgreSQL database connection
3. **Checkpoint**: State file for resumability
4. **Deduplication**: encounter_id field for uniqueness
5. **Tables**: 16 table mappings (master + 15 detail tables)
   - Each with field mapping
   - Schema definition for auto-creation
   - Filter transformer to skip empty records
6. **Notifications**: Email and webhook channels

## Tables

- `tbl_encounter_detail` - Master table with encounter status
- `tbl_event_cdi` - CDI codes
- `tbl_event_cdi_pro` - CDI professional codes
- `tbl_event_cpt_fac` - CPT facility codes
- `tbl_event_cpt_pro` - CPT professional codes
- `tbl_event_em_fac` - EM facility codes
- `tbl_event_em_pro` - EM professional codes
- `tbl_event_hcpcs_fac` - HCPCS facility codes
- `tbl_event_hcpcs_pro` - HCPCS professional codes
- `tbl_event_icd10_dx_fac` - ICD10 DX facility codes
- `tbl_event_icd10_dx_pro` - ICD10 DX professional codes
- `tbl_event_pqri_pro` - PQRI codes
- `tbl_event_pqrs_pro` - PQRS codes
- `tbl_event_special_fac` - Special facility codes
- `tbl_event_special_pro` - Special professional codes

## Running

```bash
# Navigate to examples directory
cd examples

# Run the ETL
go run medical_coding_runner.go
```

## How It Works

1. **Config Loading**: The runner reads `medical_coding_config.yaml`
2. **Job Preparation**: Manager creates one ETL job per table mapping
3. **Parallel Processing**: Each job processes the same CSV source
4. **Filtering**: Filter transformers skip records without relevant fields
5. **Transactions**: Each batch is wrapped in a database transaction
6. **Deduplication**: encounter_id prevents duplicate processing
7. **Notifications**: Success/failure notifications are sent

## CSV Format

The source CSV should have columns for all code types:
```csv
encounter_id,patient_id,service_date,cdi_code,cpt_code,cpt_pro_code,dx_fac_code,...
```

Each row represents one encounter, with sparse data (only filled for relevant codes).

## Customization

To add more tables:
1. Add a new table mapping in `medical_coding_config.yaml`
2. Define the field mapping from CSV to DB columns
3. Add a filter transformer to skip empty records
4. Define the normalize_schema for auto table creation

No code changes required!
