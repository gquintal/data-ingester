"""
Kaggle to Azure ADLS Ingester
Ejecuta en GitHub Actions - convierte datasets de Kaggle a CSV y los sube a ADLS
"""

import os
import logging
import shutil
import argparse
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


def get_kaggle_credentials(keyvault_uri):
    """Obtiene credenciales de Kaggle desde Azure Key Vault"""
    logger.info(f"Conectando a Key Vault: {keyvault_uri}")
    
    credential = DefaultAzureCredential()
    kv_client = SecretClient(vault_url=keyvault_uri, credential=credential)
    
    try:
        kaggle_user = kv_client.get_secret("kaggle-username").value
        kaggle_key = kv_client.get_secret("kaggle-key").value
        
        if not kaggle_user or not kaggle_key:
            raise ValueError("Credenciales de Kaggle vacías en Key Vault")
            
        logger.info(f"Credenciales obtenidas para usuario: {kaggle_user[:3]}***")
        return kaggle_user, kaggle_key
        
    except Exception as e:
        logger.error(f"Error obteniendo credenciales: {str(e)}")
        raise


def download_kaggle_dataset(dataset_slug, kaggle_user, kaggle_key, tmp_dir):
    """Descarga y descomprime dataset de Kaggle"""
    logger.info(f"Configurando Kaggle API para dataset: {dataset_slug}")
    
    # Configurar credenciales
    os.environ["KAGGLE_USERNAME"] = kaggle_user
    os.environ["KAGGLE_KEY"] = kaggle_key
    
    # Autenticar y descargar
    from kaggle.api.kaggle_api_extended import KaggleApi # Este import debe de hacerse aqui dado que Kaggle API se está autenticando automáticamente al importarse
    api = KaggleApi()
    api.authenticate()
    
    os.makedirs(tmp_dir, exist_ok=True)
    
    logger.info(f"Descargando dataset...")
    api.dataset_download_files(
        dataset=dataset_slug,
        path=tmp_dir,
        unzip=True
    )
    logger.info(f"Dataset descargado en: {tmp_dir}")

def connect_to_adls(storage_account, container_name):
    """Conecta a Azure Data Lake Storage"""
    logger.info(f"Conectando a ADLS: {storage_account}/{container_name}")
    
    credential = DefaultAzureCredential()
    account_url = f"https://{storage_account}.blob.core.windows.net"
    
    blob_service = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_service.get_container_client(container_name)
    
    # Verificar que el container existe
    try:
        container_client.get_container_properties()
        logger.info(f"Conexión exitosa a contenedor")
    except Exception as e:
        logger.error(f"Error conectando al contenedor: {str(e)}")
        raise
    
    return container_client

def process_and_upload_files(tmp_dir, container_client, dataset_slug):
    """Procesa archivos y los sube como CSV a ADLS"""
    logger.info(f"Procesando archivos para subida...")
    
    files_processed = 0
    files_uploaded = []
    errors = []
    
    for file_path in Path(tmp_dir).rglob("*"):
        if not file_path.is_file() or file_path.suffix.lower() == ".zip":
            continue
        
        try:
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
               # Convertir Excel a CSV (mantener nombre original)
                csv_name = f"{file_path.stem}.csv"
                csv_path = f'{tmp_dir}/{csv_name}'
                
                logger.info(f"Convirtiendo {file_path.name} → {csv_name}")
                
                # Leer Excel con pandas
                df = pd.read_excel(file_path)
                logger.info(f"Dimensiones: {df.shape[0]} filas × {df.shape[1]} columnas")
                
                # Guardar como CSV
                df.to_csv(csv_path, index=False, encoding='utf-8')
                logger.info(f"El archivo '{file_path.name}' ha sido convertido a '{csv_name}' exitosamente.")

                # Subir a ADLS
                logger.info(f"Subiendo blob: {csv_name}")
                with open(csv_path, "rb") as data:
                    container_client.upload_blob(name=csv_name, data=data, overwrite=True)
                
                files_uploaded.append(csv_name)
                files_processed += 1
                logger.info(f"Subido: {csv_name}")
                
            elif file_path.suffix.lower() == '.csv':
                # Subir CSV directamente (nombre original)
                csv_name = file_path.name
                
                with open(file_path, "rb") as data:
                    container_client.upload_blob(name=csv_name, data=data, overwrite=True)
                
                files_uploaded.append(csv_name)
                files_processed += 1
                logger.info(f"Subido: {csv_name}")
                
            else:
                logger.info(f"Ignorando {file_path.name} (formato no soportado)")
                
        except Exception as e:
            error_msg = f"Error procesando {file_path.name}: {str(e)}"
            logger.error(f"{error_msg}")
            errors.append(error_msg)
    
    # Resumen final
    logger.info(f"Proceso completado!")
    logger.info(f"Estadísticas finales:")
    logger.info("   Archivos procesados: %s", files_processed)
    logger.info("   Archivos subidos: %s", len(files_uploaded))
    if errors:
        logger.warning("   Errores: %s", len(errors))
        for error in errors[:3]:  # Mostrar máximo 3 errores
            logger.warning("      - %s", error)
    
    logger.info(f"Archivos en ADLS:")
    for file in files_uploaded:
        logger.info(f"   - {file}")
    
    return files_processed, files_uploaded, errors

def main():
    parser = argparse.ArgumentParser(description='Kaggle to ADLS Ingester')
    parser.add_argument('--dataset', required=True, 
                       help='Kaggle dataset slug (e.g., username/dataset-name)')
    parser.add_argument('--keyvault-uri', required=True, 
                       help='Azure Key Vault URI')
    parser.add_argument('--storage-account', required=True, 
                       help='Azure Storage account name')
    parser.add_argument('--container', required=True, 
                       help='Azure Storage container name')
    
    args = parser.parse_args()
    
    # Directorio temporal
    tmp_dir = "kaggle_data_tmp"
    # Obtener la ruta del proyecto (directorio donde está este script)
    project_dir = Path(__file__).parent.resolve()
    tmp_dir = os.path.join(project_dir, tmp_dir)
    
    logger.info(f" Iniciando Kaggle to ADLS Ingester")
    logger.info(f" Parámetros:")
    logger.info(f" Dataset: %s", args.dataset)
    logger.info(f" Storage: %s/%s", args.storage_account, args.container)
    logger.info(f" Key Vault: %s", args.keyvault_uri)

    try:
        # 1. Obtener credenciales
        kaggle_user, kaggle_key = get_kaggle_credentials(args.keyvault_uri)
        
        # 2. Descargar dataset
        download_kaggle_dataset(args.dataset, kaggle_user, kaggle_key, tmp_dir)
        
        # 3. Conectar a ADLS
        container_client = connect_to_adls(args.storage_account, args.container)
        
        # 4. Procesar y subir archivos
        files_processed, files_uploaded, errors = process_and_upload_files(
            tmp_dir, container_client, args.dataset
        )
        
        # 5. Validar resultado
        if files_processed == 0:
            logger.error("No se procesaron archivos. Verificar el dataset.")
            exit(1)
        elif errors and len(errors) >= files_processed:
            logger.error("Demasiados errores durante el procesamiento.")
            exit(1)
        else:
            logger.info(f"Proceso exitoso: {files_processed} archivos procesados")
            
            # Crear archivo de éxito
            try:
                success_content = f"Ingestion completed at {datetime.now(timezone.utc).isoformat()}\nFiles processed: {files_processed}\nFiles uploaded: {', '.join(files_uploaded) if files_uploaded else 'N/A'}"
                
                success_blob_name = "_SUCCESS.txt"
                container_client.upload_blob(
                    name=success_blob_name, 
                    data=success_content.encode('utf-8'), 
                    overwrite=True
                )
                logger.info(f"✅ Success marker created: {success_blob_name}")
                
            except Exception as e:
                logger.error(f"⚠️ Could not create success marker: {str(e)}")
                # No fallar por esto - el proceso principal fue exitoso
            
    except Exception as e:
        logger.error(f"Error crítico: {str(e)}", exc_info=True)
        exit(1)
        
    finally:
        # Limpiar archivos temporales
        if os.path.exists(tmp_dir):
            try:
                shutil.rmtree(tmp_dir)
                logger.info("🧹 Archivos temporales limpiados")
            except Exception as e:
                logger.warning(f"No se pudo limpiar {tmp_dir}: {str(e)}")


if __name__ == "__main__":
    main()