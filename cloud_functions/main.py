import requests
import pandas as pd
from google.cloud import bigquery

def ingest_f1_data(request):
    url = 'http://ergast.com/api/f1/2023.json'

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        races = data['MRData']['RaceTable']['Races']
        df = pd.json_normalize(races)

        schema = [
            bigquery.SchemaField("raceId", "INTEGER"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("round", "INTEGER"),
            bigquery.SchemaField("circuitId", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("url", "STRING"),
        ]

        client = bigquery.Client()

        table_id = 'your_project_id.f1_data.races'

        job_config = bigquery.LoadJobConfig(schema=schema)
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()

        return 'Ingestão concluída com sucesso!'

    except requests.exceptions.RequestException as e:
        return f"Erro ao fazer requisição para a API: {e}"
    except bigquery.GoogleCloudError as e:
        return f"Erro ao carregar dados no BigQuery: {e}"
