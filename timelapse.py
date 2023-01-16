import argparse
import logging
import os
import time

from imagery_handlers import CBERS, PlanetScope, SkySat
from script_utils import get_args, get_random_string

SCRIPT_PATH = os.path.basename(__file__)

DEFAULT_IMAGERY_TYPE = PlanetScope.__name__
DEFAULT_DATASET_DIR = "datasets/"

DEFAULT_START = "2019_01"
DEFAULT_END = "2019_12"
DEFAULT_ZOOMS = [16]
DEFAULT_DURATION = 20.0

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
        "--start",
        default=DEFAULT_START
    )
    parser.add_argument(
        "--end",
        default=DEFAULT_END
    )
    parser.add_argument(
        "--zooms",
        nargs="+",
        type=int,
        default=DEFAULT_ZOOMS
    )
    parser.add_argument(
        "--duration",
        default=DEFAULT_DURATION,
        type=float
    )
    parser.add_argument(
        "--id"
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
    dataset_dir = os.path.join(
        dataset_super_dir, dataset_id + "/"
    ).replace("\\", "/")
    log_dir = os.path.join(dataset_dir, 'logs/').replace("\\", "/")
    save_dir = os.path.join(
        dataset_dir, "data/"
    ).replace("\\", "/")
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(save_dir, exist_ok=True)
    time_str = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    log_filepath = os.path.join(
        log_dir, f"{SCRIPT_PATH}_{time_str}_{dataset_id}.log"
    ).replace('\\', '/')

    imagery_type = args["imagery_type"]
    ImageryHandler = IMAGERY_HANDLERS[imagery_type]
    img_handler = ImageryHandler(save_dir=save_dir)

    start = args["start"]
    end = args["end"]
    zooms = [int(zoom) for zoom in args["zooms"]]
    duration = float(args["duration"])

    args = get_args(
        script_path=SCRIPT_PATH, log_filepath=log_filepath, **args, 
        **img_handler.get_args(),
        dataset_id = dataset_id, time = time_str
    )

    logging.info("Making timelapses...")
    
    img_handler.make_timelapses(start=start, end=end, zooms=zooms, duration=duration)

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
