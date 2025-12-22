# Flights Forecasting Project

## Ingestion Service

### Local Ingestion

To run a local ingestion test, use the following command:

```bash
python test_local_ingestion.py
```

This will:
1. Fetch flight data from OpenSky API for the last 2 hours
2. Save the data to a local Parquet file in the `samples` directory
3. Update the database with the new record
4. Print a summary of the results

### Database

The database is initialized in the `data/ingestion.db` file. The schema is defined in the `src/ingestion/db/models.py` file.
