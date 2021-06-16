# -*- coding: utf-8 -*-

import dataiku
import logging

from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role
from dataiku.customrecipe import get_plugin_config, get_recipe_config
from cache_handler import CacheHandler
from dataframe_reverse_geocoding import add_reverse_geocode_columns, get_reverse_geocode_function
from dataframe_forward_geocoding import get_forward_geocode_function, add_forward_geocode_columns
from dku_io import get_config_reverse_geocoding, get_config_forward_geocoding

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Geocoder | %(levelname)s - %(message)s')


def geocoder_recipe(forward=True):
    # Retrieve configuration
    input_dataset = dataiku.Dataset(get_input_names_for_role('input_ds')[0])
    output_dataset = dataiku.Dataset(get_output_names_for_role('output_ds')[0])

    recipe_config = get_recipe_config()
    plugin_config = get_plugin_config()

    config = get_config_forward_geocoding(recipe_config=recipe_config, plugin_config=plugin_config) if forward else get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_forward_geocode_function(config) if forward else get_reverse_geocode_function(config)

    writer = None

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:
        for current_df in input_dataset.iter_dataframes(chunksize=max(10000, config['batch_size'])):
            current_df = add_forward_geocode_columns(cache, config, current_df, geocode_function) if forward else add_reverse_geocode_columns(cache, config, current_df, geocode_function)
            # First loop, we write the schema before creating the dataset writer
            if writer is None:
                output_dataset.write_schema_from_dataframe(current_df)
                writer = output_dataset.get_writer()
            writer.write_dataframe(current_df)

    if writer is not None:
        writer.close()
