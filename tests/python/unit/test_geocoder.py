import pickle
import pdb


def test_read_config():
    """

    :return:
    """
    params = {'input_ds': None, 'output_ds': None, 'lat_column': 'latitude', 'lng_column': 'longitude',
              'provider': 'osm', 'cache_enabled': True, 'api_key': None, 'here_app_id': None, 'here_app_code': None,
              'google_client': None, 'google_client_secret': None, 'batch_enabled': False, 'batch_size': 50,
              'features': [{'name': 'address', 'column': 'geocoder_address'},
                           {'name': 'city', 'column': 'geocoder_city'},
                           {'name': 'postal', 'column': 'geocoder_postal'},
                           {'name': 'state', 'column': 'geocoder_state'},
                           {'name': 'country', 'column': 'geocoder_country'}],
              'cache_location': '/Users/thibaultdesfontaines/Library/DataScienceStudio/dss_home/caches/plugins/geocoder/reverse',
              'cache_size': 1000000.0, 'cache_eviction': 'least-recently-stored'}
    assert True