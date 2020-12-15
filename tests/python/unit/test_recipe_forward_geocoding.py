# -*- coding: utf-8 -*-

import os
import pandas as pd
import shutil

from cache_handler import CacheHandler
from dataframe_forward_geocoding import add_forward_geocode_columns, get_forward_geocode_function
from dku_io import get_config_forward_geocoding

# Retrieve configuration


def test_no_cache_small_dataset():

    plugin_config = {'cache_location': 'original', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': 1000, 'reverse_cache_policy': 'least-frequently-used', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': True, 'batch_enabled': True, 'batch_size_bing': 50, 'batch_size_uscensus': 1000, 'column_prefix': 'geo_', 'address_column': 'geo_address', 'provider': 'osm'}

    config = get_config_forward_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_forward_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'], eviction_policy=config['cache_eviction']) as cache:

        sample_address = '203 rue de Bercy, Paris'
        nbr_samples = 50
        current_df = pd.DataFrame({
            'id': list(range(nbr_samples)),
            'geo_address': [sample_address for i in range(nbr_samples)]}
        )
        current_df = add_forward_geocode_columns(cache, config, current_df, geocode_function)
        delta = 1e-3
        assert abs(current_df.iloc[0]['geo_latitude'] - 48.84444) < delta
        assert abs(current_df.iloc[0]['geo_longitude'] - 2.371837) < delta
