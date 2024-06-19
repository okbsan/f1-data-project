import requests
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import logging

logging.basicConfig(level=logging.INFO)

def ingest_f1_data(request):
    url = 'http://ergast.com/api/f1/2023.json'

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Adicionando logs para verificar os dados recebidos
        logging.info("Dados recebidos da API: %s", data)

        races = data['MRData']['RaceTable']['Races']
        df = pd.json_normalize(races)

        # Verificar o DataFrame criado e suas colunas
        logging.info("DataFrame criado: %s", df.head())
        logging.info("Colunas do DataFrame: %s", df.columns.tolist())

        # Ajustar o esquema de acordo com as colunas presentes no DataFrame
        schema = [
            bigquery.SchemaField("season", "STRING"),
            bigquery.SchemaField("round", "INTEGER"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("raceName", "STRING"),
            bigquery.SchemaField("Circuit.circuitId", "STRING"),
            bigquery.SchemaField("Circuit.url", "STRING"),
            bigquery.SchemaField("Circuit.circuitName", "STRING"),
            bigquery.SchemaField("Circuit.Location.lat", "FLOAT"),
            bigquery.SchemaField("Circuit.Location.long", "FLOAT"),
            bigquery.SchemaField("Circuit.Location.locality", "STRING"),
            bigquery.SchemaField("Circuit.Location.country", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("FirstPractice.date", "DATE"),
            bigquery.SchemaField("FirstPractice.time", "STRING"),
            bigquery.SchemaField("SecondPractice.date", "DATE"),
            bigquery.SchemaField("SecondPractice.time", "STRING"),
            bigquery.SchemaField("ThirdPractice.date", "DATE"),
            bigquery.SchemaField("ThirdPractice.time", "STRING"),
            bigquery.SchemaField("Qualifying.date", "DATE"),
            bigquery.SchemaField("Qualifying.time", "STRING"),
            bigquery.SchemaField("Sprint.date", "DATE"),
            bigquery.SchemaField("Sprint.time", "STRING")
        ]

        client = bigquery.Client()

        table_id = 'weighty-sled-426016-i6.f1_data.races'

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append data to the table if it exists, otherwise create it
        )

        # Adicionar logs para verificar o job de carregamento
        logging.info("Iniciando o job de carregamento para a tabela %s", table_id)
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()
        logging.info("Job de carregamento concluído com sucesso.")

        return 'Ingestão concluída com sucesso!'

    except requests.exceptions.RequestException as e:
        logging.error("Erro ao fazer requisição para a API: %s", e)
        return f"Erro ao fazer requisição para a API: {e}"
    except GoogleAPIError as e:
        logging.error("Erro ao carregar dados no BigQuery: %s", e)
        return f"Erro ao carregar dados no BigQuery: {e}"
    except Exception as e:
        logging.error("Erro inesperado: %s", e)
        return f"Erro inesperado: {e}"


