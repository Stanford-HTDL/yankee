
import argparse
import asyncio
import datetime
import io
import json
import logging
import os
from collections import OrderedDict
from typing import Generator, List, Optional, Tuple

import aiohttp
# import requests
from light_pipe import AsyncGatherer, Data, Transformer
from PIL import Image, ImageDraw

from light_pipe_geo import mercantile
from light_pipe_rest import AiohttpGatherer
from sample_handlers import QuadKeyTileHandler, StandardTileHandler
from script_utils import get_random_string
from storage_handlers import (AWSStorage, GCSStorage, LocalStorage,
                              StorageHandler)
from target_handlers import GeoJsonHandler


class ImageryHandler:
    __name__ = "ImageryHandler"

    TARGET_HANDLERS = {
        GeoJsonHandler.__name__: GeoJsonHandler
    }

    STORAGE_HANDLERS = {
        LocalStorage.__name__: LocalStorage,
        GCSStorage.__name__: GCSStorage,
        AWSStorage.__name__: AWSStorage
    }

    SAMPLE_HANDLERS = {
        QuadKeyTileHandler.__name__: QuadKeyTileHandler,
        StandardTileHandler.__name__: StandardTileHandler

    }    


    def parse_args(self, parser: argparse.ArgumentParser) -> dict:
        args, _ = parser.parse_known_args()
        args = vars(args)
        return args


    def get_imagery(self):
        raise NotImplementedError("This method is not implemented for this " \
            "class. Please call one of the concrete subclasses.")


class PlanetScope(ImageryHandler):
    __name__ = "PlanetScope"

    PAPI_ONE_URL: str = "https://api.planet.com/data/v1/quick-search"
    PAPI_TWO_URL: str = "https://api.planet.com/compute/ops/orders/v2"
    PAPI_TWO_HEADERS: dict = {'content-type': 'application/json'}

    DEFAULT_TARGET_HANDLER = GeoJsonHandler.__name__
    DEFAULT_STORAGE_HANDLER = LocalStorage.__name__

    DEFAULT_MAX_CLOUD_COVER: float = 1.0
    DEFAULT_ASSET_NAMES: List = ["analytic_sr", "udm"]
    DEFAULT_ITEM_TYPES: List = ["PSScene4Band"]
    DEFAULT_MAX_ORDER_SIZE: int = 500
    DEFAULT_ORDER_BASE_NAME: str = "planet-order"
    DEFAULT_PATH_PREFIX: str = "assets"
    DEFAULT_SINGLE_ARCHIVE: bool = False
    DEFAULT_PRODUCT_BUNDLE: str = "analytic_sr"
    DEFAULT_ARCHIVE_FILENAME: str = "zipped"
    DEFAULT_EMAIL_ON_COMPLETION: bool = False

    MANIFEST_SUB_DIR: str = "order_manifest/"
    MANIFEST_NAME: str = "order_manifest.json"
    RESPONSE_MANIFEST_NAME: str = "order_responses.json"

    TIMELAPSES_SUB_DIR: str = "gifs/"
    PNGS_SUB_DIR: str = "pngs/"
    TRUNCATE = True

    VALID_FC_INDICES = [
        "ndvi", "ndwi", "msavi2", "mtvi2", "vari", "tgi"
    ]

    TILES_DIR = "tiles/"
    TILES_MANIFEST_NAME = "tiles_manifest.json"

    DEFAULT_SAMPLE_HANDLER_NAME: str = QuadKeyTileHandler.__name__

    # DEFAULT_PLANET_API_KEY: str = os.environ["PLANET_API_KEY"]


    def __init__(
        self, save_dir: str
    ):
        self.save_dir = save_dir
        args = self.parse_args()

        self.test = args["test"]

        target_handler_name = args["target_handler"]
        TargetHandler = self.TARGET_HANDLERS[target_handler_name]
        self.target_handler = TargetHandler()

        storage_handler_name = args["storage_handler"]
        StorageHandler = self.STORAGE_HANDLERS[storage_handler_name]
        self.storage_handler = StorageHandler()

        self.max_cloud_cover = args["max_cloud_cover"]
        self.asset_names = args["asset_names"]
        self.item_types = args["item_types"]
        self.max_order_size = args["max_order_size"]
        self.planet_api_key = args["planet_api_key"]

        gcs_cred_str_path = args["gcs_cred_str_path"]
        if gcs_cred_str_path:
            _, gcs_bs = self.storage_handler.get_as_bytes(gcs_cred_str_path)
            gcs_bs = gcs_bs.read()
            self.gcs_cred_str = gcs_bs.decode('UTF-8')

        self.order_base_name = args["order_base_name"]
        self.bucket = args["bucket"]
        self.path_prefix = args["path_prefix"]
        self.single_archive = args["single_archive"]
        self.product_bundle = args["product_bundle"]
        self.archive_filename = args["archive_filename"]
        self.email_on_completion = args["email_on_completion"]

        sample_handler_name = args["sample_handler"]
        SampleHandler = self.SAMPLE_HANDLERS[sample_handler_name]
        self.sample_handler = SampleHandler()

        self.args = args        


    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--test",
            default=False,
            type=bool
        )
        parser.add_argument(
            "--target-handler",
            default=self.DEFAULT_TARGET_HANDLER
        )
        parser.add_argument(
            "--storage-handler",
            default=self.DEFAULT_STORAGE_HANDLER
        )
        parser.add_argument(
            "--max-cloud-cover",
            default=self.DEFAULT_MAX_CLOUD_COVER,
            type=float
        )  
        parser.add_argument(
            "--asset-names",
            default=self.DEFAULT_ASSET_NAMES,
            nargs='+'
        )
        parser.add_argument(
            "--item-types",
            default=self.DEFAULT_ITEM_TYPES,
            nargs='+'
        )
        parser.add_argument(
            "--max-order-size",
            default=self.DEFAULT_MAX_ORDER_SIZE,
            type=int
        )
        parser.add_argument(
            "--planet-api-key",
            # required=True
        )
        parser.add_argument(
            "--gcs-cred-str-path",
            required=False
        )
        parser.add_argument(
            "--order-base-name",
            default=self.DEFAULT_ORDER_BASE_NAME
        )
        parser.add_argument(
            "--bucket",
            required=False
        )
        parser.add_argument(
            "--path-prefix",
            default=self.DEFAULT_PATH_PREFIX
        )
        parser.add_argument(
            "--single-archive",
            default=self.DEFAULT_SINGLE_ARCHIVE,
            type=bool
        )
        parser.add_argument(
            "--product-bundle",
            default=self.DEFAULT_PRODUCT_BUNDLE
        )
        parser.add_argument(
            "--archive-filename",
            default=self.DEFAULT_ARCHIVE_FILENAME
        )
        parser.add_argument(
            "--email-on-completion",
            default=self.DEFAULT_EMAIL_ON_COMPLETION
        )
        parser.add_argument(
            "--sample-handler",
            default=self.DEFAULT_SAMPLE_HANDLER_NAME
        )
        args = super().parse_args(parser=parser)
        return args


    def get_args(self):
        args = {
            **self.args, 
            **self.target_handler.args, 
            **self.storage_handler.args
        }
        return args

    
    @staticmethod
    def _make_papi_one_filters(max_cloud_cover, asset_names):
        def make_cloud_cover_filter(
            max_cloud_cover: float) -> dict:
            cloud_filter = {
                "type": "RangeFilter",
                "field_name": "cloud_cover",
                "config": {
                    "lte": max_cloud_cover
                }
            }
            return cloud_filter    

        def make_download_permission_filter() -> dict:
            permission_filter = {
            "type": "PermissionFilter",
            "config": ["assets.analytic:download"]
            }
            return permission_filter


        def make_asset_type_filter(asset_names: List[str]) -> dict:
            filters = []
            for asset_name in asset_names:
                asset_filter = {
                    "type": "AssetFilter",
                    "config": [
                        asset_name
                    ]
                }
                filters.append(asset_filter)
            and_filter = {
            "type": "AndFilter",
            "config": filters
            }
            return and_filter        


        filters = list()
        cloud_filer = make_cloud_cover_filter(max_cloud_cover=max_cloud_cover)
        filters.append(cloud_filer)
        filters.append(make_download_permission_filter())
        asset_type_filter = make_asset_type_filter(asset_names=asset_names)
        filters.append(asset_type_filter)   
        return filters


    def _make_papi_one_requests(
        self, geojson: dict, start: datetime.datetime, end: datetime.datetime,
        filters: Optional[list] = None, 
        item_types: Optional[str] = ["PSScene4Band"], 
        str_fmt: str = "%Y-%m-%d", 
    ):
        def make_geometry_filter(geometry: dict) -> dict:
            geo_filter = {
                "type": "GeometryFilter",
                "field_name": "geometry",
                "config": geometry
            }
            return geo_filter


        def make_datetime_filter(start, end) -> dict:
            date_filter = {
                "type": "DateRangeFilter", 
                "field_name": "acquired", 
                "config": {
                    "gte": start,
                    "lte": end 
                }
            }
            return date_filter 


        def make_and_filter(filters: List[dict]) -> dict:
            and_filter = {
                "type": "AndFilter",
                "config": filters
            }
            return and_filter


        def make_planet_request(item_types: list, order_filter: dict) -> dict:
            request = {
                "item_types" : item_types,
                "filter" : order_filter
            }
            return request


        def make_requests_from_geojson_start_stop(
            geojson: dict, filters: list, item_types: list, 
            start: Optional[str] = None, end: Optional[str] = None,
            start_timestamp_ext = "T00:00:00Z", end_timestamp_ext = "T00:00:00Z",
            # use_embedded_start_stop: Optional[bool] = False
        ) -> Generator:
            for feature in geojson["features"]:
                start += start_timestamp_ext
                end += end_timestamp_ext
                datetime_filter = make_datetime_filter(start, end)

                geometry = feature["geometry"]
                geo_filter = make_geometry_filter(geometry)
                req_filters = filters + [geo_filter, datetime_filter]
                and_filter = make_and_filter(req_filters)

                request = make_planet_request(item_types, and_filter)
                yield request, geojson


        if filters is None:
            max_cloud_cover = 1.0
            asset_names = "analytic_sr udm".split(" ")
            filters = self.make_filters(max_cloud_cover, asset_names)

        if start is not None:
            start = start.strftime(str_fmt)
        if end is not None:
            end = end.strftime(str_fmt)
        requests = make_requests_from_geojson_start_stop(
            geojson, filters, item_types=item_types, start=start, end=end,
        )
        yield from requests


    def make_requests(
        self, input: Tuple[Tuple[datetime.datetime, datetime.datetime], str, io.BytesIO]
    ):
        (start, end), path, bs = input
        geojson = json.loads(bs.read())
        filters = self._make_papi_one_filters(
            max_cloud_cover=self.max_cloud_cover, 
            asset_names=self.asset_names
        )
        requests = self._make_papi_one_requests(
            geojson=geojson, start=start, end=end, filters=filters, 
            item_types=self.item_types
        )
        yield from requests


    async def post_request(self, input, url, session, max_order_size, *args, **kwargs):
        def split_items(items, geojson, max_num_items = max_order_size):
            sub_list = list()
            for item in items:
                if len(sub_list) >= max_num_items:
                    yield geojson, sub_list
                    sub_list = list()
                else:
                    sub_list.append(item)
            if len(sub_list) > 0:
                yield geojson, sub_list   


        async def _post_request(request, url, session):
            async with session.post(url, json=request) as response:
                if response.ok:
                    response = await response.json()
                    features = response["features"]
                    item_ids = list()
                    for item in features:
                        id_str = item["id"]
                        item_ids.append(id_str)
                    while response["_links"]["_next"] is not None:
                        try:
                            async with session.get(response["_links"]["_next"]) as response:
                                response = await response.json()
                                features = response["features"]
                                for item in features:  
                                    id_str = item["id"]
                                    item_ids.append(id_str)
                        except:
                            break
                elif response.status == 429:
                    await asyncio.sleep(int(response.headers["Retry-After"]))  
                    item_ids = await _post_request(request, url, session)
                else:
                    response.raise_for_status()

            return item_ids

        request, geojson = input
        item_ids = None
        try:
            item_ids = await _post_request(request, url, session)
        except aiohttp.client_exceptions.ClientResponseError as error:
            raise error
        return split_items(item_ids, geojson)


    def make_order_requests(
        self, input: Tuple,  **kwargs
    ):
        def _make_order_requests(
            order_uid: str,
            geojson: dict, asset_ids: List, gcs_credentials_str: str, order_base_name: str,
            bucket: str, path_prefix: str, single_archive: Optional[bool] = False, 
            item_type: Optional[str] = "PSScene4Band", 
            product_bundle: str = "analytic_sr", archive_filename: Optional[str] = None,
            email: Optional[bool] = False, subscription_id = 0, log_request = False, 
            **kwargs
        ):
            if single_archive:
                request = {
                    "name": order_uid,
                    "subscription_id": subscription_id,
                    "products": [
                    {
                        "item_ids": asset_ids,
                        "item_type": item_type,
                        "product_bundle": product_bundle
                    }
                    ],
                    "delivery": {
                    "single_archive": single_archive,
                    "archive_filename": archive_filename,
                    "google_cloud_storage": {
                        "bucket": bucket,
                        "credentials": gcs_credentials_str,
                        "path_prefix": path_prefix
                    }
                    },
                    "notifications": {
                    "email": email
                    },
                    "order_type": "full"
                }
            else:
                request = {
                    "name": order_uid,
                    "subscription_id": subscription_id,
                    "products": [
                    {
                        "item_ids": asset_ids,
                        "item_type": item_type,
                        "product_bundle": product_bundle
                    }
                    ],
                    "delivery": {
                    "single_archive": single_archive,
                    "google_cloud_storage": {
                        "bucket": bucket,
                        "credentials": gcs_credentials_str,
                        "path_prefix": path_prefix
                    }
                    },
                    "notifications": {
                    "email": email
                    },
                    "order_type": "full"
                }
            if log_request:
                logging.info(f"Request: \n {request}")
            return order_uid, geojson, request


        order_uid, order_dict = input    
        geojson = order_dict["geojson"]
        asset_ids = order_dict["asset_ids"]
        return _make_order_requests(
            order_uid=order_uid, geojson=geojson, asset_ids=asset_ids, **kwargs
        )


    async def post_order_request(self, input, url, session, headers, **kwargs):
        async def _post_request(request, url, session, headers):
            async with session.post(url, json=request, headers=headers) as response:
                if response.ok:
                    response = await response.json()
                elif response.status == 429:
                    await asyncio.sleep(int(response.headers["Retry-After"]))  
                    response = await _post_request(request, url, session, headers)
                else:
                    response.raise_for_status()

            return response

        order_uid, geojson, request = input
        try:
            response = await _post_request(request, url, session, headers)
        except aiohttp.client_exceptions.ClientResponseError as error:
            raise error
        return order_uid, geojson, response


    def download_assets(self):
        raise NotImplementedError("This method has not been implemented yet.")           


    def get_asset_ids(self):
        data = Data(
            self.storage_handler.get_filepaths_from_dir, 
            dir=self.target_handler.targets_dir
        )
        data >> Transformer(self.storage_handler.get_as_bytes) \
                >> Transformer(self.target_handler.get_interval) \
                >> Transformer(self.make_requests) \
                >> Transformer(
                    self.post_request, parallelizer=AiohttpGatherer(login=self.planet_api_key), 
                    url=self.PAPI_ONE_URL, max_order_size=self.max_order_size
                ) \

        results = data(block=True)

        results_dict = dict()
        for result in results:
            geojson, asset_ids = result
            asset_ids_dict = {
                "geojson": geojson,
                "asset_ids": asset_ids
            }
            order_uid = get_random_string()
            results_dict[order_uid] = asset_ids_dict
        results_str = json.dumps(results_dict, ensure_ascii=False, indent=4)

        manifest_path = self.storage_handler.join_paths(
            self.save_dir, self.MANIFEST_NAME)

        self.storage_handler.set_from_string(manifest_path, results_str)  
        return manifest_path


    def get_dict_from_bs(self, bs: io.BytesIO):
        return json.loads(bs.read())


    def get_items_from_dict(self, input_dict):
        for key, value in input_dict.items():
            yield key, value

    
    def load_order_manifest(self, path):
        _, bs = self.storage_handler.get_as_bytes(path)
        order_dict =self.get_dict_from_bs(bs)
        yield from self.get_items_from_dict(order_dict)


    def order_assets(self, manifest_path):
        data = Data(
            self.load_order_manifest, path=manifest_path
        )

        data >> Transformer(
                self.make_order_requests,  gcs_credentials_str=self.gcs_cred_str,
                order_base_name=self.order_base_name, bucket=self.bucket,
                path_prefix=self.path_prefix, single_archive=self.single_archive,
                item_type=self.item_types[0], product_bundle=self.product_bundle,
                archive_filename=self.archive_filename, email=self.email_on_completion
             )
        
        if not self.test:
            data >> Transformer(
                self.post_order_request, 
                parallelizer=AiohttpGatherer(login=self.planet_api_key),
                url=self.PAPI_TWO_URL, headers=self.PAPI_TWO_HEADERS
            )
             
        results = data(block=True)

        results_dict = dict()
        for result in results:
            order_uid, geojson, response = result
            response_dict = {
                "geojson": geojson,
                "response": response
            }
            results_dict[order_uid] = response_dict
        results_str = json.dumps(results_dict, ensure_ascii=False, indent=4)

        manifest_path = self.storage_handler.join_paths(
            self.save_dir, self.RESPONSE_MANIFEST_NAME)

        self.storage_handler.set_from_string(manifest_path, results_str)  
        return manifest_path
        

    def get_imagery(self):
        manifest_path = self.get_asset_ids()
        self.order_assets(manifest_path)


    def get_tiles(self, geojson: dict, zooms, truncate):  
        geo_bounds = mercantile.geojson_bounds(geojson)
        west = geo_bounds.west
        south = geo_bounds.south
        east = geo_bounds.east
        north = geo_bounds.north

        tiles = mercantile.tiles(west, south, east, north, zooms, truncate)
        for tile in tiles:
            yield tile     


    # def _get_tiles_from_bytes(self, input, zooms, truncate):
    #     asset_id, geojson, img_bs, udm_bs = input
    #     # geojson, img_bs, udm_bs = input
    #     tiles = self.get_tiles(geojson=geojson, zooms=zooms, truncate=truncate)
    #     return asset_id, geojson, img_bs, udm_bs, tiles


    def _filter_paths_by_ext(self, paths: List[str], ext: str) -> List[str]:
        paths_filtered = list()
        for path in paths:
            if path.endswith(ext):
                paths_filtered.append(path)
        return paths_filtered


    def _get_asset_paths_from_list(
        self, input: Tuple[str, dict], paths: List[str], 
        assert_udm: Optional[bool] = True
    ):
        _, order_dict = input    
        geojson = order_dict["geojson"]
        asset_ids = order_dict["asset_ids"]

        for asset_id in asset_ids:
            img_path = None
            udm_path = None
            for path in paths:
                path_name: str = path.split("/")[-1]
                if path_name.startswith(asset_id):
                    if path_name.split(".")[0].endswith("udm"):
                        udm_path = path
                    else:
                        img_path = path
            assert img_path is not None, f"Image path not found for asset {asset_id}."
            if assert_udm:
                assert udm_path is not None, f"UDM path not found for asset {asset_id}."
            yield asset_id, geojson, img_path, udm_path


    def _get_assets_as_bytes(self, input, storage_handler: StorageHandler):
        asset_id, geojson, img_path, udm_path = input
        _, img_bs = storage_handler.get_as_bytes(img_path)
        _, udm_bs = storage_handler.get_as_bytes(udm_path)
        return asset_id, geojson, img_bs, udm_bs


    def prepare_samples(
        self, manifest_path: str, train: Optional[bool] = True,  
        from_cloud_storage: Optional[bool] = True, src_base_dir: Optional[str] = None,
        ext: str = ".tif", zooms: Optional[List[int]] = [15], truncate: Optional[bool] = True
    ):
        if from_cloud_storage:
            StorageHandler = self.STORAGE_HANDLERS[GCSStorage.__name__]
            storage_handler = StorageHandler()
        else:
            storage_handler = self.storage_handler
        paths = storage_handler.get_paths(dir=src_base_dir)
        paths = self._filter_paths_by_ext(paths, ext=ext)

        # for path in paths:
        #     print(path)
        # exit()

        data = Data(
            self.load_order_manifest, path=manifest_path
        )

        data >> Transformer(self._get_asset_paths_from_list, paths=paths) \
             >> Transformer(self._get_assets_as_bytes, storage_handler=storage_handler)
            #  >> Transformer(self._get_tiles_from_bytes, zooms=zooms, truncate=truncate)


        self.sample_handler.make_samples(
            data=data, save_dir=self.save_dir, tiles_dir=self.TILES_DIR,
            storage_handler=self.storage_handler,
            train=train, zooms=zooms, truncate=truncate
        )

        # data >> Transformer(self._save_samples, save_dir=self.save_dir)        


    def make_monthly_mosaic_interval(self, input, start, end):
        path, bs = input
        return (start, end), path, bs     


    def get_mosaic_time_str_from_start_end(self, start, end):
        dates = [start, end]
        start, end = [datetime.datetime.strptime(_, "%Y_%m") for _ in dates]
        return OrderedDict(((start + datetime.timedelta(_)).strftime(r"%Y_%m"), None) for _ in range((end - start).days)).keys()


    def make_papi_monthly_mosaic_requests(self, tiles, geojson, start, end, false_color_index):
        try:
            geojson_name = geojson["name"]
        except KeyError:
            geojson_name = "geojson"
        for tile in tiles:
            z = tile.z
            x = tile.x
            y = tile.y            
            request_urls = list()
            for year_month in self.get_mosaic_time_str_from_start_end(start, end):
                request_url = f"https://tiles.planet.com/basemaps/v1/planet-tiles/global_monthly_{year_month}_mosaic/gmap/{z}/{x}/{y}.png?api_key={self.planet_api_key}"
                if false_color_index:
                    request_url += f"&proc={false_color_index}"
                request_urls.append(request_url)
            yield request_urls, z, x, y, geojson_name


    def make_monthly_mosaic_requests(self, input, zooms, truncate, false_color_index):
        (start, end), _, bs = input
        geojson = json.loads(bs.read())
        tiles = self.get_tiles(geojson, zooms=zooms, truncate=truncate)
        requests = self.make_papi_monthly_mosaic_requests(
            tiles=tiles, geojson=geojson, start=start, end=end, false_color_index=false_color_index
        )
        yield from requests 


    # def post_monthly_mosaic_request(self, input):
    #     request_urls, z, x, y, geojson_name = input
    #     responses = list()
    #     for request_url in request_urls:
    #         response = requests.get(request_url, stream=True)
    #         response.raise_for_status()
    #         responses.append(response)
    #     return responses, z, x, y, geojson_name


    async def post_monthly_mosaic_request(self, input):
        request_urls, z, x, y, geojson_name = input
        responses = list()
        async with aiohttp.ClientSession() as session:
            for request_url in request_urls:
                async with session.get(request_url) as response:
                    response.raise_for_status()
                    content = await response.read()
                    responses.append(content)
                    # print(response)
        return responses, z, x, y, geojson_name     


    def save_responses_as_gif(self, input, start, end, duration, embed_date = True):
        responses, z, x, y, geojson_name = input
        images = list()
        if embed_date:
            dates = self.get_mosaic_time_str_from_start_end(start, end)
            responses = list(zip(dates, responses))
        for response in responses:
            if embed_date:
                date, response = response
                year, month = date.split("_")
            img_bs = io.BytesIO(response)
            img = Image.open(img_bs)  
            if embed_date: 
                draw = ImageDraw.Draw(img)
                # font = ImageFont.truetype(<font-file>, <font-size>)
                # font = ImageFont.truetype("sans-serif.ttf", 16)
                # draw.text((x, y),"Sample Text",(r,g,b))
                draw.text((0, 0),f"{year} {month} {z} {x} {y}",(255,255,255))            
            images.append(img)
        bs = io.BytesIO()
        imgs_iter = iter(images)
        first_img = next(imgs_iter)
        first_img.save(fp=bs, format='GIF', append_images=imgs_iter,
                save_all=True, duration=duration, loop=0, interlace=False,
                include_color_table=True)        
        # imageio.mimsave(bs, images, duration=duration)
        timelapse_filename = f"{geojson_name}_{z}_{x}_{y}_{start}_{end}.gif"
        path = self.storage_handler.join_paths(self.save_dir, self.TIMELAPSES_SUB_DIR, timelapse_filename)
        self.storage_handler.set_from_bytes(path, bs)     


    def save_responses_as_pngs(self, input, start, end, embed_date = True, format = "png"):
        responses, z, x, y, geojson_name = input
        images = list()
        dates = self.get_mosaic_time_str_from_start_end(start, end)
        if embed_date:
            responses = list(zip(dates, responses))
        for response in responses:
            if embed_date:
                date, response = response
                year, month = date.split("_")
            img_bs = io.BytesIO(response)
            img = Image.open(img_bs)  
            if embed_date: 
                draw = ImageDraw.Draw(img)
                # font = ImageFont.truetype(<font-file>, <font-size>)
                # font = ImageFont.truetype("sans-serif.ttf", 16)
                # draw.text((x, y),"Sample Text",(r,g,b))
                draw.text((0, 0),f"{year} {month} {z} {x} {y}",(255,255,255))            
            images.append(img)

        for date, image in list(zip(dates, images)):
            bs = io.BytesIO()
            image.save(fp=bs, format=format)
            image_path = f"{geojson_name}/{z}_{x}_{y}/{start}_{end}/{date}.{format}"
            path = self.storage_handler.join_paths(self.save_dir, self.PNGS_SUB_DIR, image_path)
            self.storage_handler.set_from_bytes(path, bs)       


    def make_timelapses(
        self, start, end, zooms, duration, false_color_index = None, 
        embed_date: Optional[bool] = True, make_gifs: Optional[bool] = True
    ):
        if false_color_index:
            assert false_color_index in self.VALID_FC_INDICES, f"False color index {false_color_index} not recognized."
        data = Data(
            self.storage_handler.get_filepaths_from_dir, 
            dir=self.target_handler.targets_dir
        )

        with data:
            data >> Transformer(self.storage_handler.get_as_bytes) \
                >> Transformer(self.make_monthly_mosaic_interval, start=start, end=end) \
                >> Transformer(self.make_monthly_mosaic_requests, zooms=zooms, 
                    truncate=self.TRUNCATE, false_color_index=false_color_index) \
                >> Transformer(self.post_monthly_mosaic_request, parallelizer=AsyncGatherer())
            if make_gifs:
                data >> Transformer(
                    self.save_responses_as_gif, start=start, end=end, 
                    duration=duration, embed_date=embed_date
                ) 
            else:
                data >> Transformer(
                    self.save_responses_as_pngs, start=start, end=end, 
                    embed_date=embed_date
                )


class CBERS(ImageryHandler):
    __name__ = "CBERS"


    def __init__(self):
        raise NotImplementedError("This class has not been implemented yet.")

class SkySat(ImageryHandler):
    __name__ = "SkySat"


    def __init__(self):
        raise NotImplementedError("This class has not been implemented yet.")
