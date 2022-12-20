import argparse
import functools
import logging
import os
import time
from typing import Callable, Dict, List, Tuple

import google.cloud.storage as storage

from .io_utils import IOUtils

__all__ = ['get_args']

SCRIPT_PATH = os.path.basename(__file__)

def arg_is_true(arg_str: str) -> bool:
    return arg_str in ("True", "TRUE", "true", "T", "t")


def arg_is_false(arg_str: str) -> bool:
    return arg_str in ("False", "FALSE", "false", "F", "f")


def logging_decorator(f) -> Callable:
    @functools.wraps(f)
    def wrapper(script_path, verbose=True, *args, **kwargs) -> Dict:
        DEFAULT_LOGGING = "INFO"
        DEFAULT_TIME_STR = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
        DEFAULT_LOG_DIR = 'logs'
        DEFAULT_LOG_FILEPATH = os.path.join(
            DEFAULT_LOG_DIR, f"{script_path}_{DEFAULT_TIME_STR}.log"
        ).replace('\\', '/')
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--logging",
            default=DEFAULT_LOGGING,
            type=str
        )
        parser.add_argument(
            "--log-filepath",
            default=DEFAULT_LOG_FILEPATH,
            help="Path to a local file to which log messages will be written.",
            type=str
        )
        p_args, _ = parser.parse_known_args()
        p_args = vars(p_args)
        fn_args = f(*args, **kwargs)
        if type(fn_args) == tuple:
                fn_args, secret_keys = fn_args

        # HERE you can append any keys which you don't want to write to a log file
        secret_keys.append("credentials")
        secret_keys.append("planet_api_key")
        secret_keys.append("PLANET_API_KEY")

        p_args = {**p_args, **fn_args}
        p_args['script_path'] = script_path

        if p_args['logging'] in ("INFO", "info", "Info", "I", "i"):
            logging.getLogger().setLevel(logging.INFO)
            log_filepath = p_args['log_filepath']
            os.makedirs(os.path.dirname(log_filepath), exist_ok=True)

            format_str = "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
            logFormatter = logging.Formatter(format_str)
            rootLogger = logging.getLogger()

            fileHandler = logging.FileHandler(log_filepath)
            fileHandler.setFormatter(logFormatter)
            rootLogger.addHandler(fileHandler)

            consoleHandler = logging.StreamHandler()
            consoleHandler.setFormatter(logFormatter)
            rootLogger.addHandler(consoleHandler)

            if verbose:
                for key, value in p_args.items():
                    if key in secret_keys:
                        logging.info(f"{key}: REDACTED FOR PRIVACY")
                    else: 
                        logging.info(f"{key}: {value}")
        return p_args
    return wrapper


def gcs_args_decorator(f) -> Callable:
    @functools.wraps(f)
    def wrapper(*args, **kwargs) -> Tuple[Dict, List]:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--gcs-vars",
            type=str,
            help="Path to a csv file containing Google Cloud Access keys. "
            "This can be a Google Cloud Storage path.",
        )
        parser.add_argument(
            "--gcs-project-name",
            help="Name of Google Cloud project.",
        )
        p_args, _ = parser.parse_known_args()
        gcs_args = IOUtils.read_csv(p_args.gcs_vars)
        p_args = vars(p_args)
        fn_args = f(*args, **kwargs)    
        p_args = {**p_args, **fn_args, **gcs_args}

        if "GOOGLE_APPLICATION_CREDENTIALS" in p_args.keys():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = p_args["GOOGLE_APPLICATION_CREDENTIALS"]
        if "CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE" in p_args.keys():
            if p_args["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] == "YES":
                os.environ["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] = "YES"

        if 'gcs_bucket' in p_args.keys() and p_args['gcs_bucket'] is not None:
            gcs_bucket = p_args['gcs_bucket']
            gcs_dir = p_args['gcs_dir']
            log_filepath = p_args['log_filepath']

            gcs_log_path = os.path.join(
                gcs_dir, log_filepath
            ).replace('\\', '/')

            assert "GOOGLE_APPLICATION_CREDENTIALS" in p_args.keys(), \
                "GOOGLE_APPLICATION_CREDENTIALS environment variable must be set \
                to use GCS."
            client = storage.Client(project=p_args['gcs_project_name'])
            bucket = client.get_bucket(gcs_bucket)
            blob = bucket.blob(gcs_log_path)
            blob.upload_from_filename(log_filepath)
        
        # Report secret keys so we don't print them or log them
        secret_keys = [
            "GS_ACCESS_KEY_ID",
            "GS_SECRET_ACCESS_KEY",
            "GOOGLE_APPLICATION_CREDENTIALS"
        ]
        return p_args, secret_keys

    # GCS_VARS = args.gcs_vars
    # GCS_PROJECT_NAME = args.gcs_project_name # Cannot be in GCS vars if vars is not local

    # args = {}
    # if GCS_VARS is not None:
    #     logging.info(f"GCS_VARS: {GCS_VARS}")
    #     args["GCS_VARS"] = GCS_VARS
    # if GCS_PROJECT_NAME is not None:
    #     logging.info(f"GCS_PROJECT_NAME: {GCS_PROJECT_NAME}")
    #     args["GCS_PROJECT_NAME"] = GCS_PROJECT_NAME

    return wrapper


@logging_decorator
@gcs_args_decorator
def get_args(**kwargs) -> Dict:
    '''
    Pass default arguments from the script into this method.
    '''
    # parser = argparse.ArgumentParser()
    # # assert "script_path" in .keys(), "Script name not passed."
    # # script_path = in_params['script_path']
    # parser.add_argument(
    #     "--script-vars",
    #     # required=True,
    #     default="_script_vars.csv",
    #     type=str,
    #     help="Path to a csv file containing variables for the script. "
    # )
    # p_args, _ = parser.parse_known_args()
    # script_args = IOUtils.read_csv(p_args.script_vars)
    # p_args = vars(p_args)
    # p_args = {**p_args, **kwargs, **script_args}
    p_args = {**p_args, **kwargs}
    return p_args


if __name__ == "__main__":
    d = get_args(script_path=SCRIPT_PATH)
    logging.info(f'Parameters received: \n {d}')