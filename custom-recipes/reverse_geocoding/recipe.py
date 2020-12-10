# -*- coding: utf-8 -*-

from dku_io import get_config
from dataframe_geocoding import get_geocode_function, geocode_processor

from cache_handler import CacheHandler

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Geocoder | %(levelname)s - %(message)s')

config = get_config()

from cache_utils import CustomTmpFile
tmp_cache = CustomTmpFile()
config['cache_location'] = tmp_cache.get_temporary_cache_dir()

geocode_function = get_geocode_function(config)

input_dataset = config['input_ds']
output_dataset = config['output_ds']

writer = None

with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'], eviction_policy=config['cache_eviction']) as cache:
    for current_df in input_dataset.iter_dataframes(chunksize=max(10000, config['batch_size'])):
        current_df = geocode_processor(cache, config, current_df, geocode_function)
        if writer is None:
            config['output_ds'].write_schema_from_dataframe(current_df)
            writer = output_dataset.get_writer()
        writer.write_dataframe(current_df)

if writer is not None:
    writer.close()
