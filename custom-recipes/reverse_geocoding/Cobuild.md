# Cobuild guidance

Use this recipe to transform latitude and longitude columns into readable addresses and geocoding metadata.

Roles:
- `input_ds`: required input dataset containing latitude and longitude values.
- `output_ds`: optional output dataset for reverse-geocoded results. Create or select it when the user wants the result persisted.

Core configuration:
- Set `lat_column` to the latitude column.
- Set `lng_column` to the longitude column.
- Set `provider` to one of the descriptor choices, such as `google`, `here`, `bing`, `mapbox`, `opencage`, or `osm`.
- Keep `cache_enabled=true` unless the user asks not to cache geocoding results.
- Set `column_prefix` when output columns need a prefix.
- Feature booleans `address`, `city`, `postal`, `state`, and `country` control which address fields are returned.

Batch behavior:
- `batch_enabled` is only relevant for `provider=bing`.
- Set `batch_size_bing` only when Bing batch mode is enabled.

Credentials:
- Some providers require provider-specific credentials.
- Use `api_key` for providers that expose the API key field.
- Use `here_app_id` and `here_app_code` for `provider=here`.
- Use `google_client` and `google_client_secret` only for Google premium-plan client credentials.
