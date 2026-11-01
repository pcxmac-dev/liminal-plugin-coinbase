# Changelog

## 1.3.0 (2026-11-01)

- Released as its own package, `liminal-plugin-coinbase`, registered under the entry point group
  `liminal.providers`; its version is the distribution's.
- Its plan and request caps are in it (`TIER`, `PER_RUN`, `PER_DAY`), no longer in the app's
  `config.PUBLIC_FEEDS`; the app's `CRYPTO_VENUE_DAYS` still limits its history (`venue_view`).
- Plugin API 1.6.

## 1.2.0 (2026-10-31)

- Built into the app (then massive-collate): its source class, client and trade stream in its
  module (plugin API 1.6).

## 1.1.0 and 1.0.0

- Built into the app (then massive-collate); see the app's history.
