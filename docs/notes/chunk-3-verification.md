# Live Verification: Vanadis Fail-Fast, Metrics Scrape, and Alert Routing

- Verified `vanadis-restate` pod is `Running` and `Ready` (3/3) in namespace `default` with clean startup logs and active Restate/health HTTP listeners.
- Confirmed VictoriaMetrics is actively scraping the `vanadis_restate_reconcile_errors_total` series via `VMPodScrape/vanadis-restate` with all standard handlers reporting `0`.
- Confirmed `vmalert` has loaded `VanadisRestateReconcileErrors` (`health: ok`, `state: inactive`), `-notifier.url=http://synapse:4020` is configured, and `-notifier.blackhole` is absent.
- Probed `synapse` alert receiver endpoints non-mutatingly: malformed payload returns HTTP 400, empty list returns HTTP 200 `{"dispatched":0,"status":"accepted"}` with no Discord notifications dispatched.
