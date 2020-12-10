# -*- coding: utf-8 -*-

from dku_io import get_config_forward
from dataframe_geocoding import get_forward_geocode_function
from dataframe_geocoding import forward_geocoding_processor
from cache_handler import CacheHandler

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Geocoder | %(levelname)s - %(message)s')

config = get_config_forward()
input_dataset = config['input_ds']
output_dataset = config['output_ds']

writer = None
geocode_function = get_forward_geocode_function(config)
# Creating a fake or real cache depending on user's choice
with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'], eviction_policy=config['cache_eviction']) as cache:
    for current_df in input_dataset.iter_dataframes(chunksize=max(10000, config['batch_size'])):
        current_df = forward_geocoding_processor(cache, config, current_df, geocode_function)
        # First loop, we write the schema before creating the dataset writer
        if writer is None:
            output_dataset.write_schema_from_dataframe(current_df)
            writer = output_dataset.get_writer()
        writer.write_dataframe(current_df)

if writer is not None:
    writer.close()
