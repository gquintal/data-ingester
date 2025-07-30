import os
import shutil
import logging
from pathlib import Path
import pandas as pd

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="kaggleingest")
def kaggleingest(req: func.HttpRequest) -> func.HttpResponse:  # Cambié el nombre de función también
    """
    Descarga un dataset de Kaggle y lo sube a un contenedor de Azure Blob Storage.
    Las credenciales y parámetros se obtienen de Azure Key Vault y variables de entorno.
    """

    logging.info("Inicio ingestión en modelo v2")

    tmp_dir = None


    try:
        # Validar variables de entorno requeridas
        required_env = ["KEY_VAULT_URI", "KAGGLE_DATASET", "DATALAKE_URI", "CONTAINER_NAME"]
        for var in required_env:
            if not os.environ.get(var):
                return func.HttpResponse(f"Falta la variable de entorno: {var}", status_code=400)
    
        # Autenticación con Managed Identity y Key Vault
        credential = DefaultAzureCredential()
        kv_uri = os.environ["KEY_VAULT_URI"]
        logging.info(f"Intentando abrir KeyVault en {kv_uri!r}")

        try:
            kv_client = SecretClient(vault_url=kv_uri, credential=credential)
            logging.info("SecretClient creado correctamente")
        except Exception as e:
            logging.error("Error al crear SecretClient", exc_info=True)
            raise

        try:
            logging.info("Leyendo secreto 'kaggle-username'")
            secret_bundle = kv_client.get_secret("kaggle-username")
            kaggle_user   = secret_bundle.value
            logging.info(f"Secreto leído OK: empieza con {kaggle_user[:3]}…")
        except Exception as e:
            logging.error("No pude leer el secreto 'kaggle-username'", exc_info=True)
            raise
        
        kaggle_key = kv_client.get_secret("kaggle-key").value


        if not kaggle_user or not kaggle_key:
            return func.HttpResponse("No se encontraron las credenciales de Kaggle en Key Vault.", status_code=500)

        # Pongo las credenciales en las vars de entorno para el CLI interno
        os.environ["KAGGLE_USERNAME"] = kaggle_user
        os.environ["KAGGLE_KEY"] = kaggle_key

        # 2. Descargar y descomprimir localmente con la API de Python
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()

        slug = os.environ["KAGGLE_DATASET"]  # ej. "jillanisofttech/flight-price-prediction-dataset"

        # Obtener la ruta del proyecto (directorio donde está este script)
        project_dir = Path(__file__).parent.resolve()
        tmp_dir = os.path.join(project_dir, "tmp")

        # Crea la carpeta si no existe
        os.makedirs(tmp_dir, exist_ok=True)

        api.dataset_download_files(
            dataset=slug,
            path=tmp_dir,
            unzip=True
        )

        logging.info(f"-> Dataset descargado y descomprimido en: {tmp_dir}")

        # 3. Conectar a tu contenedor de ADLS Gen2
        account_name = os.environ["DATALAKE_URI"]
        container_name = os.environ["CONTAINER_NAME"]

        account_url = f"https://{account_name}.blob.core.windows.net"
        blob_service = BlobServiceClient(account_url=account_url, credential=credential)
        container_client = blob_service.get_container_client(container_name)

        # 4. Subir cada archivo descomprimido
        for file_path in Path(tmp_dir).rglob("*"):

            if not file_path.is_file():
                continue
            if file_path.suffix.lower() == ".zip":
                continue  # opcional: saltar los ZIP
                
            # Define the path to your XLSX file
            xlsx_file_path = f'{tmp_dir}/{file_path.name}'

            # Define the path where you want to save the CSV file
            csv_name = f'{file_path.stem}.csv'
            csv_file_path = f'{tmp_dir}/{csv_name}'

            try:
                # Read the XLSX file into a pandas DataFrame
                df = pd.read_excel(xlsx_file_path)

                # Save the DataFrame to a CSV file
                df.to_csv(csv_file_path, index=False)  # index=False prevents writing the DataFrame index as a column
                logging.info(f"El archivo '{xlsx_file_path}' ha sido convertido a '{csv_file_path}' exitosamente.")

            except FileNotFoundError:
                logging.error(f"Error: El archivo '{xlsx_file_path}' no fue encontrado.")
                logging.info("Por favor, verifica que el nombre del archivo y la ruta sean correctos.")
            except Exception as e:
                logging.error(f"Ocurrió un error al convertir el archivo: {xlsx_file_path} {e}")

            blob_name = csv_name
            logging.info(f"Subiendo blob: {blob_name}")
            # Subir el archivo al contenedor
            with open(csv_file_path, "rb") as data:
                container_client.upload_blob(name=blob_name, data=data, overwrite=True)
        return func.HttpResponse("Dataset descargado y cargado correctamente.", status_code=200)

    except Exception as e:
        logging.exception("Error en la función KaggleIngest")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    # Limpieza final: eliminar el directorio temporal
    finally:
        if tmp_dir and os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
            logging.info(f"Directorio temporal eliminado: {tmp_dir}")
