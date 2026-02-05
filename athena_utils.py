import boto3
import pandas as pd
import time
import io
import json


def run_athena_query(query: str, name: str = '', region: str = 'us-east-1', bucket: str = 'data-lake-athena-querys') -> pd.DataFrame:
    """
    Ejecuta una consulta en Athena, guarda el resultado en Parquet en S3, lo carga en un DataFrame y limpia los recursos.

    Si la consulta no devuelve filas, retorna un DataFrame vacío.
    En caso de error, asegura la eliminación de recursos (S3 y tabla) antes de propagar la excepción.
    """
    athena = boto3.client('athena', region_name=region)
    s3 = boto3.client('s3', region_name=region)
    timestamp = int(time.time())
    table_name = f"python_table_{name}_{timestamp}"
    s3_prefix = f"python/temporales/{name}_{timestamp}/"
    parquet_output_path = f's3://{bucket}/{s3_prefix}'

    ctas_query = f"""
    CREATE TABLE {table_name}
    WITH (
      format = 'PARQUET',
      external_location = '{parquet_output_path}',
      write_compression = 'SNAPPY'
    ) AS
    {query}
    """

    # Helper to clean up resources
    def clean_up():
        # Eliminar archivos Parquet de S3
        try:
            objs = s3.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
            if objs.get('Contents'):
                delete_keys = [{'Key': obj['Key']} for obj in objs['Contents']]
                s3.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
        except ClientError as e:
            print(f"⚠️ Error al eliminar archivos de S3: {e}")

        # Eliminar la tabla en Athena
        try:
            drop_query = f"DROP TABLE IF EXISTS {table_name};"
            resp = athena.start_query_execution(
                QueryString=drop_query,
                QueryExecutionContext={'Database': 'datalake'},
                ResultConfiguration={'OutputLocation': f's3://{bucket}/'}
            )
            drop_qid = resp['QueryExecutionId']
            # Esperar a que termine el DROP
            while True:
                st = athena.get_query_execution(QueryExecutionId=drop_qid)['QueryExecution']['Status']['State']
                if st in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1)
        except ClientError as e:
            print(f"⚠️ Error al eliminar la tabla en Athena: {e}")

    # Ejecutar CTAS y procesar resultados
    try:
        resp = athena.start_query_execution(
            QueryString=ctas_query,
            QueryExecutionContext={'Database': 'datalake'},
            ResultConfiguration={'OutputLocation': f's3://{bucket}/'}
        )
        qid = resp['QueryExecutionId']
        # Esperar a que termine la ejecución
        while True:
            status = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(2)

        if status != 'SUCCEEDED':
            raise Exception(athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status'].get('StateChangeReason'))

        # Intentar leer los Parquet; si no existen, devolver df vacío
        try:
            df = pd.read_parquet(parquet_output_path, engine='pyarrow')
        except (FileNotFoundError, OSError, ValueError):
            df = pd.DataFrame()

        return df

    except Exception as e:
        # En caso de error, limpiar recursos y propagar
        clean_up()
        raise

    finally:
        # Siempre limpiar al final si hubo éxito o excepción
        clean_up()


def run_athena_query_small(query: str, region: str = 'us-east-1', 
                           bucket: str = 'data-lake-athena-querys') -> pd.DataFrame:
    """
    Ejecuta consulta en Athena y obtiene resultados directamente via API.
    Ideal para datasets pequeños (<1000 filas).
    """
    athena = boto3.client('athena', region_name=region)
    
    # Ejecutar query
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'datalake'},
        ResultConfiguration={'OutputLocation': f's3://{bucket}/temp/'}
    )
    qid = resp['QueryExecutionId']
    
    # Esperar finalización
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if state != 'SUCCEEDED':
        raise Exception(status['QueryExecution']['Status'].get('StateChangeReason'))
    
    # Obtener resultados paginados
    results = []
    paginator = athena.get_paginator('get_query_results')
    
    for page in paginator.paginate(QueryExecutionId=qid):
        for row in page['ResultSet']['Rows']:
            results.append([col.get('VarCharValue') for col in row['Data']])
    
    if not results:
        return pd.DataFrame()
    
    # Crear DataFrame (primera fila son headers)
    df = pd.DataFrame(results[1:], columns=results[0])
    return df

def run_athena_query_auto(query: str, name: str = '', threshold_mb: float = 1.0, 
                          region: str = 'us-east-1', bucket: str = 'data-lake-athena-querys') -> pd.DataFrame:
    """
    Ejecuta query y elige método según tamaño:
    - Pequeño (<threshold_mb): get_query_results()
    - Grande: CTAS + Parquet
    """
    athena = boto3.client('athena', region_name=region)
    
    # Ejecutar query simple primero
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'datalake'},
        ResultConfiguration={'OutputLocation': f's3://{bucket}/temp/'}
    )
    qid = resp['QueryExecutionId']
    
    # Esperar y verificar tamaño
    while True:
        exec_details = athena.get_query_execution(QueryExecutionId=qid)
        state = exec_details['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if state != 'SUCCEEDED':
        raise Exception(exec_details['QueryExecution']['Status'].get('StateChangeReason'))
    
    # Verificar tamaño escaneado
    stats = exec_details['QueryExecution']['Statistics']
    data_scanned_mb = stats.get('DataScannedInBytes', 0) / (1024 * 1024)
    
    # Si es pequeño, usar get_query_results
    if data_scanned_mb < threshold_mb:
        return run_athena_query_small(query, region, bucket)
    
    # Si es grande, usar tu método CTAS existente
    return run_athena_query(query, name, region, bucket)

def export_dataframe_to_s3_json(df, name, bucket='raw-data-lake-virginia', key='python/category_analysis', region='us-east-1', orient='records'):
    """
    Exporta un DataFrame como JSON y lo sube a un bucket de S3.

    Args:
        df (pd.DataFrame): DataFrame a exportar.
        name (str): Nombre del archivo JSON (sin .json).
        bucket (str): Nombre del bucket de S3.
        key (str): Carpeta dentro del bucket donde guardar el archivo.
        region (str): Región de AWS.
        orient (str): Formato de exportación JSON (por defecto 'records').
    """
    s3 = boto3.client('s3', region_name=region)
    try:
        buffer = io.StringIO()
        df.to_json(buffer, orient=orient, lines=True, force_ascii=False, date_format='iso')

        s3_key = f"{key}/{name}.json"  # <-- construir el path completo

        s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue().encode('utf-8'))
        print(f"✅ JSON exportado correctamente a s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"❌ Error al exportar JSON a S3: {str(e)}")


def import_json_from_s3(name, bucket='raw-data-lake-virginia', key='python/category_analysis',
                        region='us-east-1', orient='records') -> pd.DataFrame:
    """
    Importa un archivo JSON desde un bucket de S3 y lo convierte en DataFrame.

    Args:
        name (str): Nombre del archivo JSON (sin .json).
        bucket (str): Nombre del bucket de S3.
        key (str): Carpeta dentro del bucket donde está el archivo.
        region (str): Región de AWS.
        orient (str): Formato del JSON ('records' para líneas JSON, 'columns', etc.).

    Returns:
        pd.DataFrame: DataFrame con los datos del JSON.
    """
    s3 = boto3.client('s3', region_name=region)
    s3_key = f"{key}/{name}.json"

    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')

        if orient == 'records':
            df = pd.read_json(io.StringIO(content), orient=orient, lines=True)
        else:
            df = pd.read_json(io.StringIO(content), orient=orient)

        print(f"✅ JSON importado correctamente desde s3://{bucket}/{s3_key}")
        return df
    except Exception as e:
        print(f"❌ Error al importar JSON desde S3: {str(e)}")
        return pd.DataFrame()


def create_athena_table(table_name, s3_location, columns, database='datalake',
                        file_format='JSON', region='us-east-1', bucket='aws-athena-query-results-us-east-1-158862062418'):
    athena = boto3.client('athena', region_name=region)

    columns_def = ',\n  '.join([f'{name} {dtype}' for name, dtype in columns])

    file_format = file_format.upper()
    if file_format == 'JSON':
        serde = "org.openx.data.jsonserde.JsonSerDe"
        row_format = f"ROW FORMAT SERDE '{serde}'"
        stored_as = "STORED AS TEXTFILE"
    elif file_format == 'CSV':
        serde = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
        row_format = f"ROW FORMAT SERDE '{serde}' WITH SERDEPROPERTIES ('separatorChar' = ',')"
        stored_as = "STORED AS TEXTFILE"
    elif file_format == 'PARQUET':
        row_format = ""
        stored_as = "STORED AS PARQUET"
    else:
        raise ValueError("Formato no soportado. Usar: JSON, CSV o PARQUET.")

    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
      {columns_def}
    )
    {row_format}
    {stored_as}
    LOCATION '{s3_location}'
    TBLPROPERTIES ('has_encrypted_data'='false');
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': f's3://{bucket}/'}
    )

    execution_id = response['QueryExecutionId']
    while True:
        result = athena.get_query_execution(QueryExecutionId=execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state == 'SUCCEEDED':
        print(f"✅ Tabla '{table_name}' creada en Athena.")
    else:
        reason = result['QueryExecution']['Status'].get('StateChangeReason', '')
        print(f"❌ Error al crear la tabla: {state} - {reason}")

def columns_tupla(df):
    """
    Crea una tabla externa en Athena basándose en el esquema de un DataFrame.

    Args:
        df (pd.DataFrame): DataFrame con el esquema base.
        table_name (str): Nombre de la tabla a crear en Athena.
        s3_location (str): Ruta S3 donde están los archivos (ej: 's3://mi-bucket/mis-datos/').
        database (str): Base de datos en Athena.
        file_format (str): Formato de archivo: 'JSON', 'PARQUET' o 'CSV'.
        region (str): Región AWS.
        bucket (str): Bucket donde se guarda el log de ejecución.
    """
    import numpy as np

    # Mapear tipos de pandas a tipos de Athena
    type_map = {
        'int64': 'bigint',
        'int32': 'int',
        'float64': 'double',
        'bool': 'boolean',
        'datetime64[ns]': 'timestamp',
        'object': 'string',
        'string': 'string',
        'category': 'string',
        'float32': 'float',
        'timedelta[ns]': 'string',
    }

    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        athena_type = type_map.get(dtype, 'string')  # default a string si no se reconoce
        columns.append((col, athena_type))
    
    return columns


def upload_json_to_s3(data, name, bucket='raw-data-lake-virginia', key='python/category_analysis', region='us-east-1'):
    """
    Sube un objeto Python (dict, list, etc.) como archivo JSON a S3.

    Args:
        data: Objeto Python serializable a JSON (dict, list, etc.).
        name (str): Nombre del archivo JSON (sin .json).
        bucket (str): Nombre del bucket de S3.
        key (str): Carpeta dentro del bucket donde guardar el archivo.
        region (str): Región de AWS.
    """
    s3 = boto3.client('s3', region_name=region)
    try:
        json_str = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        s3_key = f"{key}/{name}.json"

        s3.put_object(Bucket=bucket, Key=s3_key, Body=json_str.encode('utf-8'), ContentType='application/json')
        print(f"✅ JSON subido correctamente a s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"❌ Error al subir JSON a S3: {str(e)}")


def download_json_from_s3(name, bucket='raw-data-lake-virginia', key='python/category_analysis', region='us-east-1'):
    """
    Descarga un archivo JSON desde S3 y lo retorna como objeto Python (dict/list).

    Args:
        name (str): Nombre del archivo JSON (sin .json).
        bucket (str): Nombre del bucket de S3.
        key (str): Carpeta dentro del bucket donde está el archivo.
        region (str): Región de AWS.

    Returns:
        dict/list: Objeto Python deserializado del JSON, o None si hay error.
    """
    s3 = boto3.client('s3', region_name=region)
    s3_key = f"{key}/{name}.json"

    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        print(f"✅ JSON descargado correctamente desde s3://{bucket}/{s3_key}")
        return data
    except s3.exceptions.NoSuchKey:
        print(f"⚠️ No existe el archivo s3://{bucket}/{s3_key}")
        return None
    except Exception as e:
        print(f"❌ Error al descargar JSON desde S3: {str(e)}")
        return None
