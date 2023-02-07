__author__ = "Richard Correro (richard@richardcorrero.com)"


import argparse
import logging
import os
import time

from imagery_handlers import CBERS, PlanetScope, SkySat
from script_utils import get_args, get_random_string, arg_is_true

SCRIPT_PATH = os.path.basename(__file__)

DEFAULT_IMAGERY_TYPE = PlanetScope.__name__
DEFAULT_DATASET_DIR = "datasets/"

DEFAULT_START = "2019_01"
DEFAULT_END = "2019_12"
DEFAULT_ZOOMS = [15]
DEFAULT_DURATION = 250.0
DEFAULT_EMBED_DATE = True
DEFAULT_MAKE_GIFS = True
DEFAULT_SAVE_IMAGES = True

DEFAULT_TARGET_VALUE = 1
DEFAULT_FILTER_BY_TARGET_VALUE = True

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
        "--fc-index",
        default=None
    )    
    parser.add_argument(
        "--id"
    )  
    parser.add_argument(
        "--data-dir",
        default=DEFAULT_DATASET_DIR
    )     
    parser.add_argument(
        "--embed-date",
        default=DEFAULT_EMBED_DATE,
    )
    parser.add_argument(
        "--make-gifs",
        default=DEFAULT_MAKE_GIFS,
    )            
    parser.add_argument(
        "--save-images",
        default=DEFAULT_SAVE_IMAGES,
    )
    parser.add_argument(
        "--preds-csv-path",
        default=None,
    )
    parser.add_argument(
        "--target-value",
        default=DEFAULT_TARGET_VALUE
    )
    parser.add_argument(
        "--filter-by-target-value",
        default=DEFAULT_FILTER_BY_TARGET_VALUE
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

    start = args["start"]
    end = args["end"]
    zooms = [int(zoom) for zoom in args["zooms"]]
    duration = float(args["duration"])
    false_color_index = args["fc_index"]
    embed_date = arg_is_true(args["embed_date"])
    make_gifs = arg_is_true(args["make_gifs"])
    save_images = arg_is_true(args["save_images"])
    filter_by_target_value = arg_is_true(args["filter_by_target_value"])

    preds_csv_path = args["preds_csv_path"]
    target_value = int(args["target_value"])

    args = get_args(
        script_path=SCRIPT_PATH, log_filepath=log_filepath, **args, 
        **img_handler.get_args(),
        dataset_id = dataset_id, time = time_str
    )

    logging.info("Making timelapses...")
    
    img_handler.make_timelapses(
        start=start, end=end, zooms=zooms, duration=duration, 
        false_color_index=false_color_index, embed_date=embed_date,
        make_gifs=make_gifs, save_images=save_images, preds_csv_path=preds_csv_path,
        target_value=target_value, filter_by_target_value=filter_by_target_value
    )

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
