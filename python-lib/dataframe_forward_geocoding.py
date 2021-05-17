# -*- coding: utf-8 -*-

from misc import is_empty

import geocoder
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='Plugin: Geocoder | %(levelname)s - %(message)s')


def add_forward_geocode_columns(cache, config, current_df, geocode_function):
    """
    Process an input dataframe `current_df` to add several columns with geocoding results

    :param cache:
    :param config:
    :param current_df:
    :param geocode_function:
    :return:
    """
    columns = current_df.columns.tolist()
    # Adding columns to the schema
    columns_to_append = [config[c] for c in ['latitude', 'longitude'] if not config[c] in columns]
    if columns_to_append:
        index = columns.index(config['address_column'])
        current_df = current_df.reindex(columns=columns[:index + 1] + columns_to_append + columns[index + 1:],
                                        copy=False)
    # Normal, 1 by 1 geocoding when batch is not enabled/available
    if not config['batch_enabled']:
        current_df[config['latitude']], current_df[config['longitude']] = \
            zip(*current_df.apply(perform_forward_geocode, axis=1, args=(config, geocode_function, cache)))

    # Batch creation and geocoding otherwise
    else:
        batch = []
        for i, row in current_df.iterrows():
            if len(batch) == config['batch_size']:
                perform_forward_geocode_batch(current_df, config, geocode_function, cache, batch)
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
            perform_forward_geocode_batch(current_df, config, geocode_function, cache, batch)
    return current_df


def get_forward_geocode_function(config):
    """
    Handle authentication mechanism with respect to the chosen geocoding service provider `provider_function`
    """
    provider_function = getattr(geocoder, config['provider'])

    if config['provider'] == 'here':
        return lambda address: provider_function(address, app_id=config['here_app_id'], app_code=config['here_app_code'])
    elif config['provider'] == 'google':
        return lambda address: provider_function(address, key=config['api_key'], client=config['google_client'], client_secret=config['google_client_secret'])
    elif config['batch_enabled']:
        return lambda addresses: provider_function(addresses, key=config['api_key'], method='batch', timeout=config['batch_timeout'])
    else:
        return lambda address: provider_function(address, key=config['api_key'])


def perform_forward_geocode(df, config, fun, cache):
    """
    Perform query of the forward geocoding

    First check cache for stored address, if not then apply geocoding function `fun`
    to retrieve lat / long from address

    :param df:
    :param config:
    :param fun:
    :param cache:
    :return:
    """
    # Retrieve pandas serie of address to forward geocode
    address = df[config['address_column']]
    # Results
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
            # Store result address in cache
            cache[address] = res = out.latlng
        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % (address, e))

    return res


def perform_forward_geocode_batch(df, config, fun, cache, batch):
    results = []
    try:
        results = fun(list(zip(*batch))[1])
    except Exception as e:
        logging.error("Failed to geocode the following batch: %s (%s)" % (batch, e))

    for res, orig in zip(results, batch):
        try:
            i, addr = orig
            cache[addr] = res.latlng

            df.loc[i, config['latitude']] = res.lat
            df.loc[i, config['longitude']] = res.lng
        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % (addr, e))
