import in_n_out as ino
from src.extractors.data_loader import DataLoader
from src.managers.spark_session_manager import SparkSessionManager
from src.managers.data_transformer import DataTransformer
from src.managers.s3_storage_manager import S3StorageManager


def provide_spark_session_manager() -> SparkSessionManager:
    print("Registrando SparkSessionManager")
    return SparkSessionManager()


def provide_data_loader(use_s3: bool) -> DataLoader:
    print("Registrando DataLoader")
    spark_session_manager = provide_spark_session_manager()
    return DataLoader(spark_session_manager.get_spark_session(), use_s3=use_s3)


def provide_storage_manager() -> S3StorageManager:
    print("Registrando S3StorageManager")
    spark_session_manager = provide_spark_session_manager()
    return S3StorageManager(spark_session_manager.get_spark_session())


def provide_data_transformer() -> DataTransformer:
    print("Registrando DataTransformer")
    storage_manager = provide_storage_manager()
    return DataTransformer(storage_manager)


ino.register_provider(provide_spark_session_manager)
print("SparkSessionManager registrado")


ino.register_provider(provide_data_transformer)
print("DataTransformer registrado")

ino.register_provider(provide_storage_manager)
print("S3StorageManager registrado")

print("Proveedores registrados en containers.py")
