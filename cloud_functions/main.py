import requests
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import logging
import os

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

        # Renomear colunas para remover pontos
        df.columns = [col.replace('.', '_') for col in df.columns]

        # Verificar o DataFrame criado e suas colunas
        logging.info("DataFrame criado: %s", df.head())
        logging.info("Colunas do DataFrame: %s", df.columns.tolist())

        # Convertendo tipos de dados no DataFrame
        df['round'] = pd.to_numeric(df['round'], errors='coerce').fillna(0).astype(int)
        df['Circuit_Location_lat'] = pd.to_numeric(df['Circuit_Location_lat'], errors='coerce')
        df['Circuit_Location_long'] = pd.to_numeric(df['Circuit_Location_long'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        df['FirstPractice_date'] = pd.to_datetime(df['FirstPractice_date'], errors='coerce').dt.date
        df['SecondPractice_date'] = pd.to_datetime(df['SecondPractice_date'], errors='coerce').dt.date
        df['ThirdPractice_date'] = pd.to_datetime(df['ThirdPractice_date'], errors='coerce').dt.date
        df['Qualifying_date'] = pd.to_datetime(df['Qualifying_date'], errors='coerce').dt.date
        df['Sprint_date'] = pd.to_datetime(df['Sprint_date'], errors='coerce').dt.date

        client = bigquery.Client()

        table_id = 'weighty-sled-426016-i6.f1_data.races'

        job_config = bigquery.LoadJobConfig(
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

def run_query(request):
    client = bigquery.Client()

    # Caminho para o arquivo SQL
    query_path = os.path.join(os.path.dirname(__file__), 'queries', 'my_query.sql')

    # Ler o conteúdo do arquivo SQL
    with open(query_path, 'r') as query_file:
        query = query_file.read()

    logging.info("Executando query: %s", query)

    try:
        query_job = client.query(query)
        query_job.result()  # Aguarda a conclusão da query

        logging.info("Query executada com sucesso.")
        return f"Query executada com sucesso! Número de linhas afetadas: {query_job.num_dml_affected_rows}"

    except GoogleAPIError as e:
        logging.error("Erro ao executar query no BigQuery: %s", e)
        return f"Erro ao executar query no BigQuery: {e}"
    except Exception as e:
        logging.error("Erro inesperado: %s", e)
        return f"Erro inesperado: {e}"






