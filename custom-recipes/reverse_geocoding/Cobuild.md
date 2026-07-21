# Cobuild guidance

Runtime constraints:
- Always configure `output_ds`; the runner assumes this output exists even though the descriptor marks the role optional.
- Always set `provider`; the runner rejects a missing provider even though the descriptor does not mark the parameter mandatory.
- Enable at least one of `address`, `city`, `postal`, `state`, or `country`; the runner rejects a configuration with no result feature selected.
