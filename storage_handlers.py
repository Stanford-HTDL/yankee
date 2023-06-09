__author__ = "Richard Correro (richard@richardcorrero.com)"


import argparse
import io
import os
from pathlib import Path
from typing import Generator, List, Optional, Union

from google.cloud import storage
from osgeo import gdal

gdal.UseExceptions()

class StorageHandler:
    __name__ = "StorageHandler"


    def parse_args(self, parser: argparse.ArgumentParser) -> dict:
        args, _ = parser.parse_known_args()
        args = vars(args)
        return args


class LocalStorage(StorageHandler):
    __name__ = "LocalStorage"


    def __init__(self):
        args = self.parse_args()
        # self.data_dir = args["data_dir"]

        self.args = args


    def parse_args(self):
        parser = argparse.ArgumentParser()
        # parser.add_argument(
        #     "--data-dir"
        # )              
        args = super().parse_args(parser=parser)
        return args


    # def get_filepaths_from_dir(self, dir: str) -> Generator:
    #     assert os.path.exists(dir), f"Directory {dir} does not exist."
    #     if os.path.isdir(dir):
    #         filepaths = [
    #             os.path.abspath(os.path.join(dir, x)) for x in os.listdir(dir)
    #         ]
    #     else:
    #         filepaths = [dir]
    #     for filepath in filepaths:
    #         fp = Path(filepath)
    #         yield fp


    def get_filepaths_from_dir(
        self, dir: str, extension: Optional[Union[str, None]] = None,
        ignore_hidden_files: Optional[bool] = True
    ) -> Generator:
        for root, _, files in os.walk(dir):
            for file in files:
                if ignore_hidden_files and file.startswith("."):
                    continue
                if extension is None or file.endswith(extension):
                    filepath: str = os.path.join(root, file).replace("\\", "/")
                    path: Path = Path(filepath)
                    yield path


    def get_as_bytes(self, path):
        with open(path, "rb") as f:
            bs = io.BytesIO(f.read())
        bs.seek(0)
        return path, bs


    def set_from_bytes(self, path, bs):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(bs.getbuffer())        


    def set_from_string(self, path, string):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(string)  


    def set_from_gdal_mem_dataset(
        self, out_path, dataset
    ):
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        dset_tiff_out = gdal.GetDriverByName('GTiff')
        dset_tiff_out.CreateCopy(out_path, dataset, 1)


    def join_paths(self, *args):
        path = os.path.join(*args).replace('\\', '/')
        return path


    def get_paths(self, dir: str):
        raise NotImplementedError        


class GCSStorage(StorageHandler):
    __name__ = "GCSStorage"     


    def __init__(self):
        args = self.parse_args()

        gcs_creds = args["gcs_creds"]

        if gcs_creds:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcs_creds

        self.bucket = args["gcs_bucket"]
        gcs_project_name = args["gcs_project_name"]
        self.client = storage.Client(gcs_project_name)            

        self.args = args


    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--gcs-bucket",
            required=True
        )
        parser.add_argument(
            "--gcs-project-name",
            required=True
        )
        parser.add_argument(
            "--gcs-creds",
            default=None
        )              
        args = super().parse_args(parser=parser)
        return args


    def get_paths(self, dir: str):
        if not dir[-1] == "/":
            dir += "/"
        blobs = self.client.list_blobs(self.bucket, prefix=dir)
        paths = [blob.name for blob in blobs]
        return paths


    def get_as_bytes(self, path):
        bucket = self.client.get_bucket(self.bucket)
        blob = bucket.blob(path)
        bytes = blob.download_as_bytes()

        bs = io.BytesIO()

        # If you're deserializing from a bytestring:
        bs.write(bytes)
        # Or if you're deserializing from JSON:
        # bs.write(json.loads(serialized_as_json).encode('latin-1'))
        bs.seek(0)
        return path, bs          


class AWSStorage(StorageHandler):
    __name__ = "AWSStorage"


    def __init__(self):
        raise NotImplementedError("This class has not been implemented yet.")    
