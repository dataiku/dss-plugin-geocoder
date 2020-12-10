import pickle
import pdb

from cache_handler import CacheHandler
from dataframe_geocoding import geocode_processor, get_geocode_function
import pandas as pd


def test_reverse_geocoding_processor():
    """
    Isolated test for the reverse geocoding
    :return:
    """

    config = {'lat_column': 'latitude', 'lng_column': 'longitude', 'provider': 'osm', 'cache_enabled': True,
              'api_key': None, 'here_app_id': None, 'here_app_code': None, 'google_client': None,
              'google_client_secret': None, 'batch_enabled': False, 'batch_size': 50,
              'features': [{'name': 'address', 'column': 'geo_address'},
                           {'name': 'city', 'column': 'geo_city'},
                           {'name': 'postal', 'column': 'geo_postal'},
                           {'name': 'state', 'column': 'geo_state'},
                           {'name': 'country', 'column': 'geo_country'}],
              'cache_location': None,
              'cache_size': 1000000.0, 'cache_eviction': 'least-recently-stored'}

    from cache_utils import CustomTmpFile
    tmp_cache = CustomTmpFile()
    config['cache_location'] = tmp_cache.get_temporary_cache_dir()

    cache = CacheHandler(config['cache_location'].name, enabled=config['cache_enabled'], size_limit=config['cache_size'], eviction_policy=config['cache_eviction'])

    config = config
    current_df = pd.DataFrame({'latitude': [40.64749, 40.75362, 40.80902], 'longitude': [-73.97237, -73.98377, -73.94190]})
    assert 'geo_city' not in current_df.columns
    geocode_function = get_geocode_function(config)
    current_df = geocode_processor(cache, config, current_df, geocode_function)
    assert 'geo_city' in current_df.columns
    assert current_df.iloc[0]['geo_country'] == 'United States of America'
