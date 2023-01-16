import argparse
import datetime
import io
from pathlib import Path
from typing import Tuple


class TargetHandler:
    __name__ = "TargetHandler"


    def parse_args(self, parser: argparse.ArgumentParser) -> dict:
        args, _ = parser.parse_known_args()
        args = vars(args)
        return args    


class GeoJsonHandler(TargetHandler):
    __name__ = "GeoJsonHandler"

    DEFAULT_GET_DATE_FROM_FILEPATH = True    


    def __init__(
        self
    ):

        args = self.parse_args()
        self.targets_dir = args["targets_dir"]
        self.from_filepath = args["from_filepath"]

        self.args = args        


    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--targets-dir",
            required=True
        )           
        parser.add_argument(
            "--from-filepath",
            default=self.DEFAULT_GET_DATE_FROM_FILEPATH,
            type=bool
        )
        args = super().parse_args(parser=parser)
        return args


    def get_interval_from_filepath(
        self, path, diff_days: int = 30, str_fmt: str = "%Y_%m_%d", 
        dt_pref_len: int = 10
    ):
        def get_datetime(fp: Path, str_fmt: str, dt_pref_len: int):
            dt_str = fp.name[:dt_pref_len]
            dt = datetime.datetime.strptime(dt_str, str_fmt)
            return dt

        fp = Path(path)
        dt = get_datetime(fp, str_fmt, dt_pref_len)
        td = datetime.timedelta(days=diff_days)

        start = dt - td
        end = dt + td

        return start, end        
        


    def get_interval_from_feature_attributes(self):
        raise NotImplementedError()


    def get_interval(self, input: Tuple[str, io.BytesIO]):
        path, bs = input
        if self.from_filepath:
            interval = self.get_interval_from_filepath(path)
        else:
            interval = self.get_interval_from_feature_attributes(bs)
        return interval, path, bs
