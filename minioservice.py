import os
import uuid
from datetime import timedelta
from minio import Minio
from minio.error import S3Error

from utilities import get_env_vars

MINIO_DEFAULT_DURATION = 7  # Nombre de jours pour la durée par défaut des liens signés


class MinioService:
    def __init__(self):
        env_vars = get_env_vars()

        self.minio_proxy = env_vars.get("MINIO_PROXY")
        self.minio_host = env_vars.get("MINIO_HOST")
        # Valeur par défaut
        self.minio_port = int(env_vars.get("MINIO_PORT", 9000))
        self.minio_secure = True
        self.minio_access = env_vars.get("MINIO_ACCESS_KEY")
        self.minio_secret = env_vars.get("MINIO_SECRET_KEY")
        self.minio_bucket = env_vars.get("MINIO_BUCKET_NAME")

        self.minio_client = Minio(
            # f"{self.minio_host}:{self.minio_port}",
            self.minio_host,
            access_key=self.minio_access,
            secret_key=self.minio_secret,
            secure=self.minio_secure
        )

    def generate_uuid(self):
        return str(uuid.uuid4())

    def upload_file(self, file, generate_object_url=False):
        # file_extension = file.filename.split('.')[-1]
        # object_name = f"{self.generate_uuid()}.{file_extension}"
        object_name = file.get("filename")

        # Metadonnées du fichier
        meta_data = {
            'Content-Type': file.get("content_type"),
        }

        # Upload du fichier
        self.minio_client.put_object(
            self.minio_bucket,
            object_name,
            file.get("stream"),
            file.get("content_length"),
            metadata=meta_data
        )

        object_url = None
        if generate_object_url:
            object_url = self.get_file_url(object_name)

        return {
            'minio_bucket': self.minio_bucket,
            'object_name': object_name,
            'object_url': object_url,
        }

    def get_file_url(self, object_name):
        file_url = self.minio_client.presigned_get_object(
            self.minio_bucket,
            object_name,
            expires=timedelta(days=MINIO_DEFAULT_DURATION)
        )
        # Remplacer l'hôte et le port de MinIO par le proxy
        # file_url = file_url.replace(
        #     f"http://{self.minio_host}:{self.minio_port}",
        #     f"https://{self.minio_proxy}"
        # )
        return file_url

    def get_file(self, object_name):
        try:
            return self.minio_client.get_object(self.minio_bucket, object_name)
        except S3Error as err:
            print(f"Erreur lors de la récupération du fichier : {err}")
            return None

    def delete_file(self, object_name):
        try:
            self.minio_client.remove_object(self.minio_bucket, object_name)
        except S3Error as err:
            print(f"Erreur lors de la suppression du fichier : {err}")

# Exemple d'utilisation
# minio_service = MinioService()
# response = minio_service.upload_file(file, generate_object_url=True)
