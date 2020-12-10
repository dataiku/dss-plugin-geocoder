import dataiku

from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config, get_plugin_config
import os

from cache_utils import CustomTmpFile


def get_config_forward():
    config = {'input_ds': dataiku.Dataset(get_input_names_for_role('input_ds')[0]),
              'output_ds': dataiku.Dataset(get_output_names_for_role('output_ds')[0])}

    for param in ['address_column', 'cache_enabled', 'provider', 'api_key', 'here_app_id', 'here_app_code', 'google_client', 'google_client_secret']:
        config[param] = get_recipe_config().get(param, None)

    config['batch_enabled'] = get_recipe_config().get('batch_enabled', False) \
        and (config['provider'] == 'bing' or config['provider'] == 'mapquest' or config['provider'] == 'uscensus')

    config['batch_size'] = {
        'bing': get_recipe_config().get('batch_size_bing', 50),
        'mapquest': 100,
        'uscensus': get_recipe_config().get('batch_size_uscensus', 1000)
    }.get(config['provider'], 0)

    config['batch_timeout'] = {
        'bing': 10,
        'mapquest': 30,
        'uscensus': 1800
    }.get(config['provider'], 0)

    if get_plugin_config().get('cache_location', 'original') == 'original':
        tmp_cache = CustomTmpFile()
        config['cache_location'] = tmp_cache.get_temporary_cache_dir()
    else:
        config['cache_location'] = get_plugin_config().get('cache_location_custom', '')

    config['cache_size'] = get_plugin_config().get('forward_cache_size', 1000) * 1000
    config['cache_eviction'] = get_plugin_config().get('forward_cache_policy', 'least-recently-stored')

    prefix = get_recipe_config().get('column_prefix', '')
    for column_name in ['latitude', 'longitude']:
        config[column_name] = prefix + column_name

    if config['provider'] is None:
        raise AttributeError('Please select a geocoding provider.')

    return config


def get_config():

    config = {'input_ds': dataiku.Dataset(get_input_names_for_role('input_ds')[0]),
              'output_ds': dataiku.Dataset(get_output_names_for_role('output_ds')[0])}

    for param in ['lat_column', 'lng_column', 'provider', 'cache_enabled', 'api_key', 'here_app_id', 'here_app_code', 'google_client', 'google_client_secret']:
        config[param] = get_recipe_config().get(param, None)

    config['batch_enabled'] = get_recipe_config().get('batch_enabled', False) \
        and (config['provider'] == 'bing')
    config['batch_size'] = get_recipe_config().get('batch_size_bing', 50)

    config['features'] = []
    prefix = get_recipe_config().get('column_prefix', '')

    for feature in ['address', 'city', 'postal', 'state', 'country']:
        if get_recipe_config().get(feature, False):
            config['features'].append({'name': feature, 'column': prefix + feature})

    if get_plugin_config().get('cache_location', 'original') == 'original':
        config['cache_location'] = os.environ["DIP_HOME"] + '/caches/plugins/geocoder/reverse'
    else:
        config['cache_location'] = get_plugin_config().get('cache_location_custom', '')

    config['cache_size'] = get_plugin_config().get('reverse_cache_size', 1000) * 1000
    config['cache_eviction'] = get_plugin_config().get('reverse_cache_policy', 'least-recently-stored')

    if len(config['features']) == 0:
        raise AttributeError('Please select at least one feature to extract.')

    if config['provider'] is None:
        raise AttributeError('Please select a geocoding provider.')

    return config
