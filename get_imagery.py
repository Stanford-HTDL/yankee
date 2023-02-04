__author__ = "Richard Correro (richard@richardcorrero.com)"


import argparse
import logging
import os
import time

from imagery_handlers import CBERS, PlanetScope, SkySat
from script_utils import get_args, get_random_string

SCRIPT_PATH = os.path.basename(__file__)

DEFAULT_IMAGERY_TYPE = PlanetScope.__name__
DEFAULT_DATASET_DIR = "datasets/"

IMAGERY_HANDLERS = {
    PlanetScope.__name__: PlanetScope,
    CBERS.__name__: CBERS,
    SkySat.__name__: SkySat
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--imagery-type",
        default=DEFAULT_IMAGERY_TYPE
    )
    parser.add_argument(
        "--id",
        default=None
    )
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATASET_DIR
    )    
    p_args, _ = parser.parse_known_args()
    return p_args    


def main():
    args = vars(parse_args())    
    if not args["id"]:
        dataset_id = get_random_string()
    else:
        dataset_id = args["id"]
    dataset_super_dir = args["data_dir"]
    time_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())  
    dataset_dir = os.path.join(
        dataset_super_dir, dataset_id, f"{time_str}/"
    ).replace("\\", "/")
    log_dir = os.path.join(dataset_dir, 'logs/').replace("\\", "/")
    save_dir = os.path.join(
        dataset_dir, "data/"
    ).replace("\\", "/")
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(save_dir, exist_ok=True)
    # time_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    log_filepath = os.path.join(
        log_dir, f"{SCRIPT_PATH}_{time_str}_{dataset_id}.log"
    ).replace('\\', '/')

    imagery_type = args["imagery_type"]
    ImageryHandler = IMAGERY_HANDLERS[imagery_type]
    img_handler = ImageryHandler(save_dir=save_dir)

    args = get_args(
        script_path=SCRIPT_PATH, log_filepath=log_filepath, **args, 
        **img_handler.get_args(),
        dataset_id = dataset_id, time = time_str
    )

    logging.info("Getting imagery...")
    
    img_handler.get_imagery()    

    logging.info(
        """
                ================
                =              =
                =     Done.    =
                =              =
                ================
        """
    )


if __name__ == "__main__":
    main()
