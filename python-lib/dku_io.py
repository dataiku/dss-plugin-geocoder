# -*- coding: utf-8 -*-

import logging

from cache_utils import CustomTmpFile

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Geocoder | %(levelname)s - %(message)s')


def get_config_forward_geocoding(plugin_config, recipe_config):
    """
    Retrieve and process the input configuration to determine the geocoding service provider, cache settings and
    names of result columns headers for forward geocoding
    :return:
    """
    processed_config = {}
    for param in ['address_column', 'cache_enabled', 'provider', 'api_key', 'here_app_id', 'here_app_code', 'google_client', 'google_client_secret']:
        processed_config[param] = recipe_config.get(param, None)

    processed_config['batch_enabled'] = recipe_config.get('batch_enabled', False) \
                                        and (processed_config['provider'] == 'bing' or processed_config['provider'] == 'mapquest' or processed_config['provider'] == 'uscensus')

    processed_config['batch_size'] = {
        'bing': recipe_config.get('batch_size_bing', 50),
        'mapquest': 100,
        'uscensus': recipe_config.get('batch_size_uscensus', 1000)
    }.get(processed_config['provider'], 0)

    processed_config['batch_timeout'] = {
        'bing': 10,
        'mapquest': 30,
        'uscensus': 1800
    }.get(processed_config['provider'], 0)

    if plugin_config.get('cache_location', 'original') == 'original':
        # Detect an empty cache_location in the user settings
        # Will use the UIF safe cache location by default
        tmp_cache = CustomTmpFile()
        processed_config['using_default_cache'] = True
        persistent_cache_location = tmp_cache.get_cache_location_from_user_config()
        processed_config['cache_location'] = persistent_cache_location
        logger.info("Using default cache at location {}".format(persistent_cache_location))
    else:
        processed_config['using_default_cache'] = False
        processed_config['cache_location'] = plugin_config.get('cache_location_custom', '')
        logger.info("Using custom cache location {}".format(processed_config['cache_location']))

    processed_config['cache_size'] = plugin_config.get('forward_cache_size', 1000) * 1000
    processed_config['cache_eviction'] = plugin_config.get('forward_cache_policy', 'least-recently-stored')

    prefix = recipe_config.get('column_prefix', '')
    for column_name in ['latitude', 'longitude']:
        processed_config[column_name] = prefix + column_name

    if processed_config['provider'] is None:
        raise AttributeError('Please select a geocoding provider.')

    return processed_config


def get_config_reverse_geocoding(plugin_config, recipe_config):
    """
    Retrieve and process the input configuration to determine the geocoding service provider, cache settings and
    names of result columns headers for reverse geocoding
    :return:
    """

    processed_config = {}

    for param in ['lat_column', 'lng_column', 'provider', 'cache_enabled', 'api_key', 'here_app_id', 'here_app_code', 'google_client', 'google_client_secret']:
        processed_config[param] = recipe_config.get(param, None)

    processed_config['batch_enabled'] = recipe_config.get('batch_enabled', False) \
        and (processed_config['provider'] == 'bing')
    processed_config['batch_size'] = recipe_config.get('batch_size_bing', 50)

    processed_config['features'] = []
    prefix = recipe_config.get('column_prefix', '')

    for feature in ['address', 'city', 'postal', 'state', 'country']:
        if recipe_config.get(feature, False):
            processed_config['features'].append({'name': feature, 'column': prefix + feature})

    if plugin_config.get('cache_location', 'original') == 'original':
        # Detect an empty cache_location in the user settings
        # Will use the UIF safe cache location by default
        tmp_cache = CustomTmpFile()
        processed_config['using_default_cache'] = True
        persistent_cache_location = tmp_cache.get_cache_location_from_user_config()
        processed_config['cache_location'] = persistent_cache_location
        logger.info("Using default cache at location {}".format(persistent_cache_location))
    else:
        processed_config['using_default_cache'] = False
        processed_config['cache_location'] = plugin_config.get('cache_location_custom', '')
        logger.info("Using custom cache location {}".format(processed_config['cache_location']))

    processed_config['cache_size'] = plugin_config.get('reverse_cache_size', 1000) * 1000
    processed_config['cache_eviction'] = plugin_config.get('reverse_cache_policy', 'least-recently-stored')

    if len(processed_config['features']) == 0:
        raise AttributeError('Please select at least one feature to extract.')

    if processed_config['provider'] is None:
        raise AttributeError('Please select a geocoding provider.')

    return processed_config
