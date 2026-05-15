## What

<!-- 1-2 sentence summary of the change -->

## Why

<!-- Why this change is needed; link to issue if applicable -->

## Testing

<!-- How you validated the change -->

## Checklist

- [ ] **If you touched `os_updater/dispatch.py`:** open a follow-up PR in [`sslivins/agora-cms`](https://github.com/sslivins/agora-cms) to bump the vendored copy at `tests/contract/device_dispatch_validator.py` (header SHA + body). CMS mirrors `DispatchPayload` for wire-compat tests; the vendored copy must track upstream or `agora-cms` CI will fail loudly on a drift detector.
