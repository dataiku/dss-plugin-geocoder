import dataframe_forward_geocoding as forward_geocoding
import dataframe_reverse_geocoding as reverse_geocoding
import pytest
from dku_io import get_config_forward_geocoding, get_config_reverse_geocoding


def record_provider_calls(monkeypatch, module, provider: str) -> list:
    """Mocks a geocoder library provider and records its calls."""
    calls = []

    def provider_function(*args, **kwargs) -> None:
        """Captures arguments passed to the mocked provider."""
        calls.append((args, kwargs))

    monkeypatch.setattr(module.geocoder, provider, provider_function)
    return calls


@pytest.mark.parametrize(
    "provider, config, argument, expected_kwargs",
    [
        pytest.param(
            "opencage",
            {
                "batch_enabled": False,
                "use_preset": True,
                "api_key_preset": {"api_key": "preset-api-key"},
                "api_key": "should-not-be-passed",
            },
            " 203 Rue de Bercy, 75012 Paris",
            {"key": "preset-api-key"},
            id="api-key",
        ),
        pytest.param(
            "google",
            {
                "batch_enabled": False,
                "use_preset": True,
                "google_geocoder_preset": {
                    "api_key": "preset-google-key",
                    "google_client": "preset-google-client",
                    "google_client_secret": "preset-google-secret",
                },
                "api_key": "should-not-be-passed",
                "google_client": "should-not-be-passed",
                "google_client_secret": "should-not-be-passed",
            },
            " 203 Rue de Bercy, 75012 Paris",
            {
                "key": "preset-google-key",
                "client": "preset-google-client",
                "client_secret": "preset-google-secret",
            },
            id="google",
        ),
        pytest.param(
            "here",
            {
                "batch_enabled": False,
                "use_preset": True,
                "here_geocoder_preset": {
                    "here_app_id": "preset-here-app-id",
                    "here_app_code": "preset-here-app-code",
                },
                "here_app_id": "should-not-be-passed",
                "here_app_code": "should-not-be-passed",
            },
            " 203 Rue de Bercy, 75012 Paris",
            {
                "app_id": "preset-here-app-id",
                "app_code": "preset-here-app-code",
            },
            id="here",
        ),
    ],
)
def test_forward_geocode_function_uses_preset(
    monkeypatch, provider: str, config: dict, argument: str, expected_kwargs: dict
) -> None:
    """Checks that forward geocoding uses credentials from the selected preset."""
    config = dict(config)
    config["provider"] = provider
    calls = record_provider_calls(monkeypatch, forward_geocoding, provider)

    geocode_function = forward_geocoding.get_forward_geocode_function(config)
    geocode_function(argument)

    assert calls == [((argument,), expected_kwargs)]


@pytest.mark.parametrize(
    "provider, config, arguments, expected_arguments, expected_kwargs",
    [
        pytest.param(
            "opencage",
            {
                "batch_enabled": False,
                "use_preset": True,
                "api_key_preset": {"api_key": "preset-api-key"},
                "api_key": "should-not-be-passed",
            },
            (48.8444456, 2.3719039),
            ([48.8444456, 2.3719039],),
            {"method": "reverse", "key": "preset-api-key"},
            id="api-key",
        ),
        pytest.param(
            "google",
            {
                "batch_enabled": False,
                "use_preset": True,
                "google_geocoder_preset": {
                    "api_key": "preset-google-key",
                    "google_client": "preset-google-client",
                    "google_client_secret": "preset-google-secret",
                },
                "api_key": "should-not-be-passed",
                "google_client": "should-not-be-passed",
                "google_client_secret": "should-not-be-passed",
            },
            (48.8444456, 2.3719039),
            ([48.8444456, 2.3719039],),
            {
                "method": "reverse",
                "key": "preset-google-key",
                "client": "preset-google-client",
                "client_secret": "preset-google-secret",
            },
            id="google",
        ),
        pytest.param(
            "here",
            {
                "batch_enabled": False,
                "use_preset": True,
                "here_geocoder_preset": {
                    "here_app_id": "preset-here-app-id",
                    "here_app_code": "preset-here-app-code",
                },
                "here_app_id": "should-not-be-passed",
                "here_app_code": "should-not-be-passed",
            },
            (48.8444456, 2.3719039),
            ([48.8444456, 2.3719039],),
            {
                "method": "reverse",
                "app_id": "preset-here-app-id",
                "app_code": "preset-here-app-code",
            },
            id="here",
        ),
    ],
)
def test_reverse_geocode_function_uses_preset(
    monkeypatch,
    provider: str,
    config: dict,
    arguments: tuple,
    expected_arguments: tuple,
    expected_kwargs: dict,
) -> None:
    """Checks that reverse geocoding uses credentials from the selected preset."""
    config = dict(config)
    config["provider"] = provider
    calls = record_provider_calls(monkeypatch, reverse_geocoding, provider)

    geocode_function = reverse_geocoding.get_reverse_geocode_function(config)
    geocode_function(*arguments)

    assert calls == [(expected_arguments, expected_kwargs)]


@pytest.mark.parametrize(
    "module, factory, provider, config, arguments, expected_arguments, expected_kwargs",
    [
        pytest.param(
            forward_geocoding,
            forward_geocoding.get_forward_geocode_function,
            "google",
            {
                "batch_enabled": False,
                "api_key": "inline-google-key",
                "google_client": "inline-google-client",
                "google_client_secret": "inline-google-secret",
                "google_geocoder_preset": {
                    "api_key": "should-not-be-passed",
                    "google_client": "should-not-be-passed",
                    "google_client_secret": "should-not-be-passed",
                },
            },
            (" 203 Rue de Bercy, 75012 Paris",),
            (" 203 Rue de Bercy, 75012 Paris",),
            {
                "key": "inline-google-key",
                "client": "inline-google-client",
                "client_secret": "inline-google-secret",
            },
            id="forward-without-use-preset",
        ),
        pytest.param(
            reverse_geocoding,
            reverse_geocoding.get_reverse_geocode_function,
            "here",
            {
                "batch_enabled": False,
                "use_preset": False,
                "here_app_id": "inline-here-app-id",
                "here_app_code": "inline-here-app-code",
                "here_geocoder_preset": {
                    "here_app_id": "should-not-be-passed",
                    "here_app_code": "should-not-be-passed",
                },
            },
            (48.8444456, 2.3719039),
            ([48.8444456, 2.3719039],),
            {
                "method": "reverse",
                "app_id": "inline-here-app-id",
                "app_code": "inline-here-app-code",
            },
            id="reverse-with-use-preset-disabled",
        ),
    ],
)
def test_geocode_function_uses_inline_credentials_when_preset_is_disabled(
    monkeypatch,
    module,
    factory,
    provider: str,
    config: dict,
    arguments: tuple,
    expected_arguments: tuple,
    expected_kwargs: dict,
) -> None:
    """Checks that disabled presets preserve legacy inline authentication."""
    config = dict(config)
    config["provider"] = provider
    calls = record_provider_calls(monkeypatch, module, provider)

    geocode_function = factory(config)
    geocode_function(*arguments)

    assert calls == [(expected_arguments, expected_kwargs)]


@pytest.mark.parametrize(
    "config_factory, recipe_config",
    [
        pytest.param(
            get_config_forward_geocoding,
            {
                "address_column": "address",
                "cache_enabled": False,
                "column_prefix": "geo_",
                "provider": "opencage",
            },
            id="forward",
        ),
        pytest.param(
            get_config_reverse_geocoding,
            {
                "address": True,
                "cache_enabled": False,
                "column_prefix": "geo_",
                "lat_column": "latitude",
                "lng_column": "longitude",
                "provider": "opencage",
            },
            id="reverse",
        ),
    ],
)
def test_config_preserves_preset_configuration(config_factory, recipe_config: dict) -> None:
    """Checks that processing preserves preset fields from the recipe configuration."""
    preset_fields_to_preserve = {
        "use_preset": True,
        "api_key_preset": {"api_key": "preset-api-key"},
        "google_geocoder_preset": {
            "api_key": "preset-google-key",
            "google_client": "preset-google-client",
            "google_client_secret": "preset-google-secret",
        },
        "here_geocoder_preset": {
            "here_app_id": "preset-here-app-id",
            "here_app_code": "preset-here-app-code",
        },
    }
    recipe_config = dict(recipe_config)
    recipe_config.update(preset_fields_to_preserve)
    plugin_config = {
        "cache_location": "custom",
        "cache_location_custom": "/tmp/geocoder-test-cache",
    }

    # Processes the recipe configuration through the function under test.
    config = config_factory(plugin_config, recipe_config)

    # Verifies that every preset field is preserved in the processed configuration.
    for name, value in preset_fields_to_preserve.items():
        assert config[name] == value
