import requests
import pandas as pd
from google.cloud import bigquery

def ingest_f1_data(request):
    # URL da API da Ergast para dados de corridas de Fórmula 1
    url = 'http://ergast.com/api/f1/2023.json'

    try:
        # Fazendo requisição para a API
        response = requests.get(url)
        response.raise_for_status()  # Verifica se houve erros na requisição
        data = response.json()

        # Processando os dados
        races = data['MRData']['RaceTable']['Races']
        df = pd.json_normalize(races)

        # Esquema da tabela
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

        # Instanciando cliente do BigQuery
        client = bigquery.Client()

        # Referência à tabela
        table_id = 'your_project_id.f1_data.races'  # Substitua 'your_project_id' pelo ID do seu projeto

        # Carregando dados para o BigQuery
        job_config = bigquery.LoadJobConfig(schema=schema)
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Aguarda o job terminar

        return 'Ingestão concluída com sucesso!'

    except requests.exceptions.RequestException as e:
        return f"Erro ao fazer requisição para a API: {e}"
    except exceptions.GoogleCloudError as e:
        return f"Erro ao carregar dados no BigQuery: {e}"
