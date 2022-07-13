import google.cloud.bigquery as bq
import pandas_gbq

def load_chunks(
    client,
    dataframe,
    dataset_id,
    table_id,
    chunksize=None,
    schema=None,
    location=None,
):
    destination_table = client.dataset(dataset_id).table(table_id)
    job_config = bq.LoadJobConfig()
    job_config.write_disposition = "WRITE_APPEND"
    job_config.source_format = "CSV"
    job_config.allow_quoted_newlines = True

    if schema is None:
        schema = pandas_gbq.schema.generate_bq_schema(dataframe)

    schema = pandas_gbq.schema.add_default_nullable_mode(schema)

    job_config.schema = [
        bq.SchemaField.from_api_repr(field) for field in schema["fields"]
    ]