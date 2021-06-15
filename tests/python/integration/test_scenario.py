# -*- coding: utf-8 -*-
from dku_plugin_test_utils import dss_scenario


TEST_PROJECT_KEY = "PLUGINTESTGEOCODER"


def test_forward_geocoding_osm_nocache(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="forward-geocoding-osm-nocache")

def test_forward_geocoding_osm_partition(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="forward-geocoding-osm-partition")

def test_forward_geocoding_osm_sql(user_dss_clients):
    dss_scenario.run(user_dss_clients, project_key=TEST_PROJECT_KEY, scenario_id="forward-geocoding-osm-sql")