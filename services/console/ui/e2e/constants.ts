// Shared by the fixture generator, the Go server launcher and the Playwright config/specs.

/** The instant both UIs are pinned to (page.clock.setFixedTime); every fixture is relative to it. */
export const FIXTURE_NOW = '2026-11-08T12:00:00.000Z';

/** Port the real console binary listens on during the suite. */
export const E2E_PORT = 4387;

export const BASE_URL = `http://127.0.0.1:${E2E_PORT}`;
