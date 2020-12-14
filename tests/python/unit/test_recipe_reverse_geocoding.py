# -*- coding: utf-8 -*-

import os
import pandas as pd
import pytest
import shutil
import time

from cache_handler import CacheHandler
from dataframe_reverse_geocoding import add_reverse_geocode_columns, get_reverse_geocode_function
from dku_io import get_config_reverse_geocoding


def test_no_cache_small_dataset():

    plugin_config = {'cache_location': 'original', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': 1000, 'reverse_cache_policy': 'least-frequently-used', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': False, 'batch_enabled': False, 'batch_size_bing': 50, 'address': True, 'city': True, 'postal': True, 'state': True, 'country': True, 'column_prefix': 'geo_', 'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm'}

    config = get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_reverse_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:

        N = 10
        current_df = pd.DataFrame({
            'latitude': [40.64749 for i in range(N)],
            'longitude': [-73.97237 for i in range(N)]
        })

        start_time = time.time()
        current_df = add_reverse_geocode_columns(cache, config, current_df, geocode_function)
        assert current_df.iloc[0]['geo_postal'] == '11218'
        job_duration = time.time() - start_time

    if recipe_config['cache_enabled']:
        # Delete cache location after job
        if config['using_default_cache']:
            tmp_cache = config['cache_handler']
            tmp_cache.clean()
            cache_location = tmp_cache.tmp_output_dir.name
            assert not os.path.isdir(cache_location)
        else:
            shutil.rmtree(plugin_config['cache_location_custom'])


@pytest.mark.skip(reason="Test takes too much time")
def test_large_cache_large_ds():

    plugin_config = {'cache_location': 'original', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': 1000000, 'reverse_cache_policy': 'least-recently-stored', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': True, 'batch_enabled': True, 'batch_size_bing': 50, 'address': True, 'city': True, 'postal': True, 'state': True, 'country': True, 'column_prefix': 'geo_', 'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm'}

    config = get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_reverse_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:

        current_df = pd.DataFrame({
            'latitude': [40.64749 for i in range(2000000)],
            'longitude': [-73.97237 for i in range(2000000)]
        })
        current_df = add_reverse_geocode_columns(cache, config, current_df, geocode_function)
        assert current_df.iloc[0]['geo_postal'] == '11218'

    if recipe_config['cache_enabled']:
        # Delete cache location after job
        if config['using_default_cache']:
            tmp_cache = config['cache_handler']
            tmp_cache.clean()
            cache_location = tmp_cache.tmp_output_dir.name
            assert not os.path.isdir(cache_location)
        else:
            shutil.rmtree(plugin_config['cache_location_custom'])


@pytest.mark.skip(reason="Test takes too much time")
def test_small_cache_small_ds():

    plugin_config = {'cache_location': 'original', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': 1, 'reverse_cache_policy': 'least-recently-stored', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': True, 'batch_enabled': True, 'batch_size_bing': 50, 'address': True, 'city': True, 'postal': True, 'state': True, 'country': True, 'column_prefix': 'geo_', 'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm'}

    config = get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_reverse_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:

        current_df = pd.DataFrame({
            'latitude': [40.64749],
            'longitude': [-73.97237]
        })
        current_df = add_reverse_geocode_columns(cache, config, current_df, geocode_function)
        assert current_df.iloc[0]['geo_postal'] == '11218'

    if recipe_config['cache_enabled']:
        # Delete cache location after job
        if config['using_default_cache']:
            tmp_cache = config['cache_handler']
            tmp_cache.clean()
            cache_location = tmp_cache.tmp_output_dir.name
            assert not os.path.isdir(cache_location)
        else:
            shutil.rmtree(plugin_config['cache_location_custom'])


def test_small_cache_size():
    """
    This test is long
    :return:
    """
    plugin_config = {'cache_location': 'original', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': 10, 'reverse_cache_policy': 'least-frequently-used', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': True, 'batch_enabled': True, 'batch_size_bing': 50, 'address': True, 'city': True, 'postal': True, 'state': True, 'country': True, 'column_prefix': 'geo_', 'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm'}

    config = get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_reverse_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:
        N = 1000
        current_df = pd.DataFrame({
            'latitude': [40.64749 for i in range(N)],
            'longitude': [-73.97237 for i in range(N)]
        })
        current_df = add_reverse_geocode_columns(cache, config, current_df, geocode_function)
        assert current_df.iloc[0]['geo_postal'] == '11218'

    if recipe_config['cache_enabled']:
        # Delete cache location after job
        if config['using_default_cache']:
            tmp_cache = config['cache_handler']
            tmp_cache.clean()
            cache_location = tmp_cache.tmp_output_dir.name
            assert not os.path.isdir(cache_location)
        else:
            shutil.rmtree(plugin_config['cache_location_custom'])


def test_null_cache_size():

    plugin_config = {'cache_location': 'original', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': 0, 'reverse_cache_policy': 'least-recently-stored', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': True, 'batch_enabled': True, 'batch_size_bing': 50, 'address': True, 'city': True, 'postal': True, 'state': True, 'country': True, 'column_prefix': 'geo_', 'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm'}

    config = get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_reverse_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:

        current_df = pd.DataFrame({
            'latitude': [40.64749],
            'longitude': [-73.97237]
        })
        current_df = add_reverse_geocode_columns(cache, config, current_df, geocode_function)
        assert current_df.iloc[0]['geo_postal'] == '11218'

    if recipe_config['cache_enabled']:
        # Delete cache location after job
        if config['using_default_cache']:
            tmp_cache = config['cache_handler']
            tmp_cache.clean()
            cache_location = tmp_cache.tmp_output_dir.name
            assert not os.path.isdir(cache_location)
        else:
            shutil.rmtree(plugin_config['cache_location_custom'])


def test_negative_cache_size():

    plugin_config = {'cache_location': 'original', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': -1, 'reverse_cache_policy': 'least-recently-stored', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': True, 'batch_enabled': True, 'batch_size_bing': 50, 'address': True, 'city': True, 'postal': True, 'state': True, 'country': True, 'column_prefix': 'geo_', 'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm'}
    config = get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_reverse_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:

        current_df = pd.DataFrame({
            'latitude': [40.64749],
            'longitude': [-73.97237]
        })
        current_df = add_reverse_geocode_columns(cache, config, current_df, geocode_function)
        assert current_df.iloc[0]['geo_postal'] == '11218'

    if recipe_config['cache_enabled']:
        # Delete cache location after job
        if config['using_default_cache']:
            tmp_cache = config['cache_handler']
            tmp_cache.clean()
            cache_location = tmp_cache.tmp_output_dir.name
            assert not os.path.isdir(cache_location)
        else:
            shutil.rmtree(plugin_config['cache_location_custom'])



def test_cache_not_enabled():

    plugin_config = {'cache_location': 'original', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': 1000000, 'reverse_cache_policy': 'least-recently-stored', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': False, 'batch_enabled': True, 'batch_size_bing': 50, 'address': True, 'city': True, 'postal': True, 'state': True, 'country': True, 'column_prefix': 'geo_', 'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm'}
    config = get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_reverse_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:

        current_df = pd.DataFrame({
            'latitude': [40.64749],
            'longitude': [-73.97237]
        })
        current_df = add_reverse_geocode_columns(cache, config, current_df, geocode_function)
        assert current_df.iloc[0]['geo_postal'] == '11218'

    if recipe_config['cache_enabled']:
        # Delete cache location after job
        if config['using_default_cache']:
            tmp_cache = config['cache_handler']
            tmp_cache.clean()
            cache_location = tmp_cache.tmp_output_dir.name
            assert not os.path.isdir(cache_location)
        else:
            shutil.rmtree(plugin_config['cache_location_custom'])


def test_large_default_cache():

    plugin_config = {'cache_location': 'original', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': 1000000, 'reverse_cache_policy': 'least-recently-stored', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': True, 'batch_enabled': True, 'batch_size_bing': 50, 'address': True, 'city': True, 'postal': True, 'state': True, 'country': True, 'column_prefix': 'geo_', 'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm'}
    config = get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_reverse_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:
        current_df = pd.DataFrame({
            'latitude': [40.64749],
            'longitude': [-73.97237]
        })
        current_df = add_reverse_geocode_columns(cache, config, current_df, geocode_function)
        assert current_df.iloc[0]['geo_postal'] == '11218'

    if recipe_config['cache_enabled']:
        # Delete cache location after job
        if config['using_default_cache']:
            tmp_cache = config['cache_handler']
            tmp_cache.clean()
            cache_location = tmp_cache.tmp_output_dir.name
            assert not os.path.isdir(cache_location)
        else:
            shutil.rmtree(plugin_config['cache_location_custom'])


def test_large_custom_cache():

    plugin_config = {'cache_location': 'custom', 'forward_cache_size': 1000, 'forward_cache_policy': 'least-recently-stored', 'reverse_cache_size': 1000000, 'reverse_cache_policy': 'least-recently-stored', 'cache_location_custom': './tmp/cache/'}
    recipe_config = {'cache_enabled': True, 'batch_enabled': True, 'batch_size_bing': 50, 'address': True, 'city': True, 'postal': True, 'state': True, 'country': True, 'column_prefix': 'geo_', 'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm'}
    config = get_config_reverse_geocoding(recipe_config=recipe_config, plugin_config=plugin_config)
    geocode_function = get_reverse_geocode_function(config)

    with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], size_limit=config['cache_size'],
                      eviction_policy=config['cache_eviction']) as cache:
        current_df = pd.DataFrame({
            'latitude': [40.64749],
            'longitude': [-73.97237]
        })
        current_df = add_reverse_geocode_columns(cache, config, current_df, geocode_function)
        assert current_df.iloc[0]['geo_postal'] == '11218'

    if recipe_config['cache_enabled']:
        # Delete cache location after job
        if config['using_default_cache']:
            tmp_cache = config['cache_handler']
            tmp_cache.clean()
            cache_location = tmp_cache.tmp_output_dir.name
            assert not os.path.isdir(cache_location)
        else:
            shutil.rmtree(plugin_config['cache_location_custom'])



