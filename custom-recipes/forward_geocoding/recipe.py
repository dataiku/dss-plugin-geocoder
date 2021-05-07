# -*- coding: utf-8 -*-

import dataiku
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config, get_plugin_config
from cache_handler import CacheHandler

import geocoder
import numpy as np
import logging
import os
import sys

# Logging setup
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
project_key = dataiku.default_project_key()
client = dataiku.api_client()
project = client.get_project(project_key)
dss_cache = project.get_cache()

def get_config():
    config = {}
    config['input_ds'] = dataiku.Dataset(get_input_names_for_role('input_ds')[0])
    config['output_ds'] = dataiku.Dataset(get_output_names_for_role('output_ds')[0])

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
        config['cache_location'] = os.environ["DIP_HOME"] + '/caches/plugins/geocoder/forward'
    else:
        config['cache_location'] = get_plugin_config().get('cache_location_custom', '')

    config['cache_size'] = get_plugin_config().get('forward_cache_size', 1000) * 1000
    config['cache_eviction'] = get_plugin_config().get('forward_cache_policy', 'least-recently-stored')

    prefix = get_recipe_config().get('column_prefix', '')
    for column_name in ['latitude', 'longitude']:
        config[column_name] = prefix + column_name

    if config['provider'] is None:
        raise AttributeError('Please select a geocoding provider.')

    # for batch cache
    config['dss_cache_batch_enabled'] = get_recipe_config().get('dss_cache_batch_enabled')
    if config['dss_cache_batch_enabled']:
        config['batch_size'] = get_recipe_config().get('cache_batch_chunk_size')
    return config


def get_geocode_function(config):
    provider_function = getattr(geocoder, config['provider'])

    if config['provider'] == 'here':
        return lambda address: provider_function(address, app_id=config['here_app_id'], app_code=config['here_app_code'])
    elif config['provider'] == 'google':
        return lambda address: provider_function(address, key=config['api_key'], client=config['google_client'], client_secret=config['google_client_secret'])
    elif config['batch_enabled']:
        return lambda addresses: provider_function(addresses, key=config['api_key'], method='batch', timeout=config['batch_timeout'])
    else:
        return lambda address: provider_function(address, key=config['api_key'])


def is_empty(val):
    if isinstance(val, float):
        return np.isnan(val)
    else:
        return not val


def perform_geocode(df, config, fun, cache):
    address = df[config['address_column']]
    res = [None, None]

    try:
        if any([is_empty(df[config[c]]) for c in ['latitude', 'longitude']]):
            res = cache[address]
        else:
            res = [df[config[c]] for c in ['latitude', 'longitude']]

    except KeyError:
        try:
            out = fun(address)
            if not out.latlng:
                raise Exception('Failed to retrieve coordinates')

            cache[address] = res = out.latlng
        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % (address, e))

    return res


def perform_geocode_batch(df, config, fun, cache, batch):
    results = []
    try:
        results = fun(zip(*batch)[1])
    except Exception as e:
        logging.error("Failed to geocode the following batch: %s (%s)" % (batch, e))

    print('results', results)
    for res, orig in zip(results, batch):
        try:
            i, addr = orig
            print('res.latlng', res.latlng)
            cache[addr] = res.latlng

            df.loc[i, config['latitude']] = res.lat
            df.loc[i, config['longitude']] = res.lng
        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % (addr, e))


def main():
    config = get_config()
    geocode_function = get_geocode_function(config)

    writer = None

    try:
        # Creating a fake or real cache depending on user's choice
        with CacheHandler(config['cache_location'], enabled=config['cache_enabled'],
                          size_limit=config['cache_size'], eviction_policy=config['cache_eviction']) as cache:
            for current_df in config['input_ds'].iter_dataframes(chunksize=max(10000, config['batch_size'])):
                columns = current_df.columns.tolist()

                # Adding columns to the schema
                columns_to_append = [config[c] for c in ['latitude', 'longitude'] if not config[c] in columns]
                if columns_to_append:
                    index = columns.index(config['address_column'])
                    current_df = current_df.reindex(columns=columns[:index + 1] + columns_to_append + columns[index + 1:], copy=False)

                if config['dss_cache_batch_enabled']:
                    addresses = list(current_df[config['address_column']])
                    cache_resp = dss_cache.get_batch(addresses)
                    print('cache_resp', cache_resp)
                    cache_resolved = cache_resp['hits']
                    miss_resolved = {}
                    if True or not config['batch_enabled']:
                        print('calling API for these adresses', cache_resp['misses'])
                        for miss in cache_resp['misses']:
                            out = geocode_function(miss)
                            miss_resolved[miss] = out.latlng
                            print(miss[:5], "=>", out.latlng)
                    dss_cache.set_entry_batch(miss_resolved)
                    resolved = {}
                    resolved.update(miss_resolved)
                    resolved.update(cache_resolved)
                    print('resolved', resolved)
                    for i, row in current_df.iterrows():
                        print('writing', resolved[row[config['address_column']]])
                        if resolved[row[config['address_column']]] is not None:
                            current_df.loc[i, config['latitude']] = resolved[row[config['address_column']]][0]
                            current_df.loc[i, config['longitude']] = resolved[row[config['address_column']]][1]


                # Normal, 1 by 1 geocoding when batch is not enabled/available
                elif not config['batch_enabled']:
                    current_df[config['latitude']], current_df[config['longitude']] = \
                        zip(*current_df.apply(perform_geocode, axis=1, args=(config, geocode_function, cache)))

                # Batch creation and geocoding otherwise
                else:
                    batch = []

                    for i, row in current_df.iterrows():
                        if len(batch) == config['batch_size']:
                            perform_geocode_batch(current_df, config, geocode_function, cache, batch)
                            batch = []

                        address = row[config['address_column']]
                        try:
                            if any([is_empty(row[config[c]]) for c in ['latitude', 'longitude']]):
                                res = cache[address]
                            else:
                                res = [row[config[c]] for c in ['latitude', 'longitude']]

                            current_df.loc[i, config['latitude']] = res[0]
                            current_df.loc[i, config['longitude']] = res[1]
                        except KeyError:
                            batch.append((i, address))

                    if len(batch) > 0:
                        perform_geocode_batch(current_df, config, geocode_function, cache, batch)

                # First loop, we write the schema before creating the dataset writer
                if writer is None:
                    config['output_ds'].write_schema_from_dataframe(current_df)
                    writer = config['output_ds'].get_writer()

                writer.write_dataframe(current_df)
    finally:
        if writer is not None:
            writer.close()


main()
