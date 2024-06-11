import os
import requests
from pyspark.sql import SparkSession
from config.config import Config
from decorators.decorators import log_decorator, timing_decorator
from tqdm import tqdm


class DataLoader:
    """
        Clase para cargar datos desde una fuente externa a un DataFrame de Spark.

        Esta clase demuestra la adherencia al principio de Inversión de Dependencias (DIP) de SOLID,
        al recibir la sesión de Spark como una dependencia externa en lugar de crearla internamente.
        Esto permite que la clase sea más flexible y desacoplada, facilitando la prueba y la extensión
        de la funcionalidad sin modificar esta clase base.

        Atributos:
            spark_session (SparkSession): Una sesión activa de Spark utilizada para leer datos.

        Métodos:
            load_data: Carga datos desde una ubicación remota especificada y los retorna como un DataFrame.
        """
    def __init__(self, spark_session: SparkSession):
        """
                Inicializa la clase DataLoader con una sesión de Spark.

                Args:
                    spark_session (SparkSession): La sesión de Spark a utilizar para operaciones de datos.
        """
        self.spark_session = spark_session

    @log_decorator
    @timing_decorator
    def load_data(self):
        """
                Carga datos desde un archivo remoto y los convierte en un DataFrame de Spark.

                Este método gestiona la descarga de datos desde una URL remota si el archivo local no existe,
                lo cual es clave para garantizar que los datos siempre estén actualizados y listos para el procesamiento.
                Una vez descargados, los datos se leen en un DataFrame de Spark usando la sesión pasada en el constructor.

                Returns:
                    DataFrame: Un DataFrame de Spark conteniendo los datos cargados.
        """
        local_file_path = Config.get_data_path(Config.LOCAL_FILENAME)
        if not os.path.exists(local_file_path):
            print("Downloading data...")
            response = requests.get(Config.REMOTE_DATA_URL, stream=True)
            total_size = int(response.headers.get('content-length', 0))
            with open(local_file_path, 'wb') as f:
                for data in tqdm(response.iter_content(1024), total=total_size // 1024, unit='KB'):
                    f.write(data)

        print("Loading data from local file...")
        df = self.spark_session.read.csv(local_file_path, header=True, inferSchema=True)
        return df
