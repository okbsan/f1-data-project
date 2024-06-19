import requests
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

def ingest_f1_data(request):
    url = 'http://ergast.com/api/f1/2023.json'

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Adicionando logs para verificar os dados recebidos
        print("Dados recebidos da API:", data)

        races = data['MRData']['RaceTable']['Races']
        df = pd.json_normalize(races)

        print("DataFrame criado:", df.head())

        schema = [
            bigquery.SchemaField("raceId", "STRING"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("round", "INTEGER"),
            bigquery.SchemaField("circuitId", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("url", "STRING"),
        ]

        client = bigquery.Client()

        table_id = 'weighty-sled-426016-i6.f1_data.races'

        job_config = bigquery.LoadJobConfig(schema=schema)
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()

        print("Dados carregados com sucesso no BigQuery.")

        return 'Ingestão concluída com sucesso!'

    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer requisição para a API: {e}")
        return f"Erro ao fazer requisição para a API: {e}"
    except GoogleAPIError as e:
        print(f"Erro ao carregar dados no BigQuery: {e}")
        return f"Erro ao carregar dados no BigQuery: {e}"

