# Cobuild guidance

Use this recipe to transform address strings into coordinates and geocoding metadata.

Roles:
- `input_ds`: required input dataset containing address values.
- `output_ds`: optional output dataset for geocoded results. Create or select it when the user wants the result persisted.

Core configuration:
- Set `address_column` to the input address column.
- Set `provider` to one of the descriptor choices, such as `google`, `here`, `bing`, `mapbox`, `opencage`, or `osm`.
- Keep `cache_enabled=true` unless the user asks not to cache geocoding results.
- Set `column_prefix` when output columns need a prefix.

Batch behavior:
- `batch_enabled` is only relevant for providers that support batch mode, including `bing`, `mapquest`, and `uscensus`.
- Set `batch_size_bing` or `batch_size_uscensus` only when the corresponding provider is selected and batch mode is enabled.

Credentials:
- Some providers require provider-specific credentials.
- Use `api_key` for providers that expose the API key field.
- Use `here_app_id` and `here_app_code` for `provider=here`.
- Use `google_client` and `google_client_secret` only for Google premium-plan client credentials.
