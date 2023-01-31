__author__ = "Richard Correro (richard@richardcorrero.com)"


import argparse
import io
import json
from typing import Generator, List

import numpy as np
from light_pipe import Data, Optional, Transformer
from osgeo import gdal, ogr

from light_pipe_geo import gridding, mercantile
from script_utils import get_random_string
from storage_handlers import StorageHandler

gdal.UseExceptions()
ogr.UseExceptions()


class SampleHandler:
    __name__ = "SampleHandler"


    def parse_args(self, parser: argparse.ArgumentParser) -> dict:
        args, _ = parser.parse_known_args()
        args = vars(args)
        return args    


class QuadKeyTileHandler(SampleHandler):
    __name__ = "QuadKeyTileHandler"

    TILES_MANIFEST_NAME = "tiles_manifest.json"

    def __init__(
        self
    ):

        args = self.parse_args()

        self.args = args        


    def parse_args(self):
        parser = argparse.ArgumentParser()
        # @TODO: Add args
        args = super().parse_args(parser=parser)
        return args


    def get_tiles(self, geojson: dict, zooms, truncate):  
        geo_bounds = mercantile.geojson_bounds(geojson)
        west = geo_bounds.west
        south = geo_bounds.south
        east = geo_bounds.east
        north = geo_bounds.north

        tiles = mercantile.tiles(west, south, east, north, zooms, truncate)
        for tile in tiles:
            yield tile             


    # @TODO: IMPLEMENT THIS
    def make_tile_datasets(
        self, input, pixel_x_meters: Optional[float] = 3.0, 
        pixel_y_meters: Optional[float] = -3.0, train: Optional[bool] = True
    ):
        asset_id, geojson, img_bs, udm_bs, tiles = input
        if train:
            geojson_bytes = json.dumps(geojson).encode('utf-8')
            geojson_ds = ogr.Open(geojson_bytes)
        img_ds_gen = self._bytes_to_dataset(img_bs)
        img_ds = next(img_ds_gen)

        udm_ds_gen = self._bytes_to_dataset(udm_bs)
        udm_ds = next(udm_ds_gen)
        for tile in tiles:
            zoom = tile.z

            quad_key = mercantile.quadkey(tile)

            # out_sub_dir = os.path.join(
            #     tiles_dir, "zoom_" + str(zoom), quad_key + '/'
            # )
            # os.makedirs(out_sub_dir, exist_ok=True)
            # out_udm_path = os.path.join(out_sub_dir, f"{asset_id}_udm.tif")
            # out_target_path = os.path.join(out_sub_dir, f"{asset_id}_target.tif")
            # out_geotiff_path = os.path.join(out_sub_dir, f"{asset_id}_geotiff.tif")

            # Make datasets
            if train:
                qkey, (geojson_grid_cell_dataset, _, _) = gridding.make_grid_cell_dataset(
                    grid_cell=tile, datum=geojson_ds, return_filepaths=False, is_label=True, 
                    in_memory=True, pixel_x_meters=pixel_x_meters, pixel_y_meters=pixel_y_meters,
                    # grid_cell_filepath=out_target_path
                )
    
            qkey, (geotiff_grid_cell_dataset, _, _) = gridding.make_grid_cell_dataset(
                    grid_cell=tile, datum=img_ds, return_filepaths=False, is_label=False, 
                    in_memory=True, pixel_x_meters=pixel_x_meters, pixel_y_meters=pixel_y_meters,
                    # grid_cell_filepath=out_geotiff_path
            )

            qkey, (udm_grid_cell_dataset, _, _) = gridding.make_grid_cell_dataset(
                grid_cell=tile, datum=udm_ds, return_filepaths=False, is_label=True, 
                in_memory=True, pixel_x_meters=pixel_x_meters, pixel_y_meters=pixel_y_meters,
                # grid_cell_filepath=out_udm_path,
                no_data_value=1
            )
            if not train:
                yield zoom, quad_key, asset_id, None, \
                    geotiff_grid_cell_dataset, udm_grid_cell_dataset
            else:
                yield zoom, quad_key, asset_id, geojson_grid_cell_dataset, \
                    geotiff_grid_cell_dataset, udm_grid_cell_dataset
        
        try:
            next(img_ds_gen)
            next(udm_ds_gen)
        except StopIteration:
            pass

        geojson_ds = None


    def make_synthetic_masks(
        self, input,
        binary: Optional[bool] = True
    ):
        zoom, quad_key, asset_id, geojson_grid_cell_dataset, \
            geotiff_grid_cell_dataset, udm_grid_cell_dataset = input
        # Write new synthetic mask to `out_target_path`
        udm_arr = udm_grid_cell_dataset.ReadAsArray()
        dataset =  geojson_grid_cell_dataset
        target_arr = dataset.ReadAsArray()

        if binary:
            target_arr[target_arr > 0] = 1
            target_arr[udm_arr >= 1] = 0
        else:
            target_arr[target_arr > 0] = 3
            target_arr[udm_arr == 1] = 1
            target_arr[udm_arr > 1] = 2
        dataset.WriteArray(target_arr)
        # del(dataset)
        dataset = None

        if np.allclose(target_arr, 0):
            # return f"{out_target_path} contains only null values."
            all_null = True
        else:
            all_null = False

        return all_null, zoom, quad_key, asset_id, geojson_grid_cell_dataset, \
            geotiff_grid_cell_dataset, udm_grid_cell_dataset


    def _get_tiles_from_bytes(self, input, zooms, truncate):
        asset_id, geojson, img_bs, udm_bs = input
        # geojson, img_bs, udm_bs = input
        tiles = self.get_tiles(geojson=geojson, zooms=zooms, truncate=truncate)
        return asset_id, geojson, img_bs, udm_bs, tiles       


    def _save_samples(
        self, input, save_dir: str, tiles_dir: str, storage_handler: StorageHandler,
        train: Optional[bool] = True
    ):
        """
        1. Make paths
        2. Save gdal Datasets to paths
        3. return paths
        """
        if train:
            all_null, zoom, quad_key, asset_id, geojson_grid_cell_dataset, \
                geotiff_grid_cell_dataset, udm_grid_cell_dataset = input
        else:
            zoom, quad_key, asset_id, geojson_grid_cell_dataset, \
                geotiff_grid_cell_dataset, udm_grid_cell_dataset = input

        out_sub_dir = storage_handler.join_paths(
            save_dir, tiles_dir, "zoom_" + str(zoom), quad_key + '/'
        )
        # os.makedirs(out_sub_dir, exist_ok=True)
        out_udm_path = storage_handler.join_paths(out_sub_dir, f"{asset_id}_udm.tif")
        out_target_path = storage_handler.join_paths(out_sub_dir, f"{asset_id}_target.tif")
        out_geotiff_path = storage_handler.join_paths(out_sub_dir, f"{asset_id}_geotiff.tif")
        if train:
            storage_handler.set_from_gdal_mem_dataset(out_target_path, geojson_grid_cell_dataset)
        storage_handler.set_from_gdal_mem_dataset(out_udm_path, udm_grid_cell_dataset)
        storage_handler.set_from_gdal_mem_dataset(out_geotiff_path, geotiff_grid_cell_dataset)
        return all_null, zoom, quad_key, asset_id, out_udm_path, out_target_path, out_geotiff_path             


    def _bytes_to_dataset(
        self, bs: io.BytesIO, vsi_path: str = '/vsimem/tiffinmem' + get_random_string()
    ) -> Generator:
        bs.seek(0)
        gdal.FileFromMemBuffer(vsi_path, bs.getbuffer()) 
        ds = gdal.Open(vsi_path)
        yield ds
        
        del(ds)
        gdal.Unlink(vsi_path)


    def make_samples(
        self, data: Data, save_dir: str, tiles_dir: str, 
        storage_handler: StorageHandler, train: bool, zooms: List[int], 
        truncate: Optional[bool] = True
    ) -> Data:
        # data >> Transformer(self.make_tile_datasets) \
        #      >> Transformer(self.make_synthetic_masks)
        data >> Transformer(self._get_tiles_from_bytes, zooms=zooms, truncate=truncate) \
             >> Transformer(self.make_tile_datasets, train=train)

        if train:
            data >> Transformer(self.make_synthetic_masks)

        data >> Transformer(
            self._save_samples, save_dir=save_dir, tiles_dir=tiles_dir,
            train=train, storage_handler=storage_handler
        )

        results = data(block=True)
        results_dict = dict()
        for result in results:
            all_null, zoom, quad_key, asset_id, out_udm_path, out_target_path, out_geotiff_path = result
            paths_dict = {
                "target": out_target_path.replace("\\", "/"),
                "image": out_geotiff_path.replace("\\", "/"),
                "udm": out_udm_path.replace("\\", "/"),
                "all_null": all_null,

            }
            if zoom not in results_dict.keys():
                results_dict[zoom] = dict()
            zoom_dict = results_dict[zoom]
            if quad_key not in zoom_dict.keys():
                zoom_dict[quad_key] = dict()
            quad_key_dict = zoom_dict[quad_key]
            quad_key_dict[asset_id] = paths_dict

        results_bs = json.dumps(results_dict).encode('utf-8')
        results_bs = io.BytesIO(results_bs)
        samples_manifest_path = storage_handler.join_paths(save_dir, self.TILES_MANIFEST_NAME)
        storage_handler.set_from_bytes(samples_manifest_path, results_bs)  


class StandardTileHandler(SampleHandler):
    __name__ = "StandardTileHandler"


    def __init__(
        self
    ):
        raise NotImplementedError("This class is not implemented yet.")
