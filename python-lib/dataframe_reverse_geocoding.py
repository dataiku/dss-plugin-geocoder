# -*- coding: utf-8 -*-

from misc import is_empty

import geocoder
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Geocoder | %(levelname)s - %(message)s')


def add_reverse_geocode_columns(cache, config, current_df, geocode_function):
    columns = current_df.columns.tolist()
    columns_to_append = [f['column'] for f in config['features'] if not f['column'] in columns]
    if columns_to_append:
        index = max(columns.index(config['lat_column']), columns.index(config['lng_column']))
        current_df = current_df.reindex(columns=columns[:index + 1] + columns_to_append + columns[index + 1:],
                                        copy=False)
    if not config['batch_enabled']:
        results = zip(*current_df.apply(perform_reverse_geocode, axis=1, args=(config, geocode_function, cache)))

        for feature, result in zip(config['features'], results):
            current_df[feature['column']] = result

    else:
        batch = []

        for i, row in current_df.iterrows():
            if len(batch) == config['batch_size']:
                perform_reverse_geocode_batch(current_df, config, geocode_function, cache, batch)
                batch = []

            lat = row[config['lat_column']]
            lng = row[config['lng_column']]

            try:
                if any([is_empty(row[f['column']]) for f in config['features']]):
                    res = cache[(lat, lng)]
                else:
                    res = {}
                    for f in config['features']:
                        res[f['name']] = row[f['column']]

                for feature in config['features']:
                    current_df.loc[i, feature['column']] = res[feature['name']]

            except KeyError as e:
                batch.append((i, (lat, lng)))

        if len(batch) > 0:
            perform_reverse_geocode_batch(current_df, config, geocode_function, cache, batch)

    # First loop, we write the schema before creating the dataset writer
    return current_df


def get_reverse_geocode_function(config):
    provider_function = getattr(geocoder, config['provider'])
    
    use_preset = config.get('use_preset', False)
    api_key = config.get('api_key', '')
    here_config = { 'here_app_id': config['here_app_id'], 'here_app_code': config['here_app_code'] }
    google_config = { 'api_key': config['api_key'], 'google_client': config['google_client'], 'google_client_secret': config['google_client_secret'] }
    if use_preset:
        api_key = config.get('api_key_preset', {}).get('api_key', '')
        here_config = config.get('here_geocoder_preset', {})
        google_config = config.get('google_geocoder_preset', {})

    if config['provider'] == 'here':
        return lambda lat, lng: provider_function([lat, lng], method='reverse', app_id=here_config.get('here_app_id', ''), app_code=here_config.get('here_app_code', ''))
    elif config['provider'] == 'google':
        return lambda lat, lng: provider_function([lat, lng], method='reverse', key=google_config.get('api_key', ''), client=google_config.get('google_client', ''), client_secret=google_config.get('google_client_secret', ''))
    elif config['batch_enabled']:
        return lambda locations: provider_function(locations, method='batch_reverse', key=api_key)
    else:
        return lambda lat, lng: provider_function([lat, lng], method='reverse', key=api_key)


def perform_reverse_geocode(df, config, fun, cache):
    lat = df[config['lat_column']]
    lng = df[config['lng_column']]
    res = {'address': None, 'city': None, 'postal': None, 'state': None, 'country': None}

    try:
        if any([is_empty(df[f['column']]) for f in config['features']]):
            res = cache[(lat, lng)]
            logging.info('Hit cache')
        else:
            for f in config['features']:
                res[f['name']] = df[f['column']]

    except KeyError:
        logging.info('Missed cache')
        try:
            out = fun(lat, lng)
            if not any([out.address, out.city, out.postal, out.state, out.country]):
                raise Exception('Failed to retrieve coordinates')

            for feature in res.keys():
                val = getattr(out, feature)
                res[feature] = val

            cache[(lat, lng)] = res
        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % ((lat, lng), e))

    formatted_res = [res[feature['name']] for feature in config['features']]

    return formatted_res


def perform_reverse_geocode_batch(df, config, fun, cache, batch):

    results = []
    try:
        results = fun(list(zip(*batch))[1])
    except Exception as e:
        logging.error("Failed to geocode the following batch: %s (%s)" % (batch, e))

    for out, orig in zip(results, batch):
        try:
            if not out.address and not out.city and not out.postal and not out.state and not out.country:
                raise Exception('Failed to retrieve coordinates')

            res = {'address': None, 'city': None, 'postal': None, 'state': None, 'country': None}

            for feature in res.keys():
                val = getattr(out, feature)
                res[feature] = val

            i, loc = orig
            cache[(loc[0], loc[1])] = res

            for feature in config['features']:
                df.loc[i, feature['column']] = res[feature['name']]

        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % (loc, e))
