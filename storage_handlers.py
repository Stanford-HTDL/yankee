import argparse
import io
import os
from pathlib import Path
from typing import Generator


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


    def get_filepaths_from_dir(self, dir: str) -> Generator:
        assert os.path.exists(dir), f"Directory {dir} does not exist."
        if os.path.isdir(dir):
            filepaths = [
                os.path.abspath(os.path.join(dir, x)) for x in os.listdir(dir)
            ]
        else:
            filepaths = [dir]
        for filepath in filepaths:
            fp = Path(filepath)
            yield fp  


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


    def join_paths(self, *args):
        path = os.path.join(*args)
        return path


class GCSStorage(StorageHandler):
    __name__ = "GCSStorage"


    def __init__(self):
        raise NotImplementedError("This class has not been implemented yet.")      


class AWSStorage(StorageHandler):
    __name__ = "AWSStorage"


    def __init__(self):
        raise NotImplementedError("This class has not been implemented yet.")    
