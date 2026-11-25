---
name: kotlin-wear-android
description: Use when writing, reviewing, building, or testing Kotlin (.kt, .kts), Android, Jetpack Compose, Wear OS Compose Material3, Glance Wear widgets/tiles, AndroidKeyStore, or NFC HostApduService (HCE) applications.
---

# Kotlin, Android & Wear OS 5+ Engineering Standards

Synthesized from Google's official [`android/skills`](https://github.com/android/skills) (`wear-compose-m3`, `wear-widgets`, `jetpack-compose-performance`, `android-intent-security`, `android-permissions-security`), JetBrains' [`Kotlin/kotlin-agent-skills`](https://github.com/Kotlin/kotlin-agent-skills), [`maxrave-dev/kotlin-footguns`](https://github.com/maxrave-dev/kotlin-footguns), and CSA Aliro / Android HCE hardware standards.

---

## 1. Toolchain, Gradle & Verification Invariants

- **Kotlin 2.x & Compose Compiler**: Use Kotlin **2.0+** with the official `org.jetbrains.kotlin.plugin.compose` Gradle plugin and centralized version catalogs (`gradle/libs.versions.toml`). Read versions directly from `libs.versions.toml` or `build.gradle.kts` (never run `./gradlew dependencies` just to inspect versions).
- **Mandatory Full Assembly & Unit Test Gate**:
  - Always verify builds with `./gradlew :app:assembleDebug :app:testDebugUnitTest` (or containerized Android SDK Gradle wrapper).
  - **NEVER rely on `./gradlew compileDebugKotlin` alone**—it skips `:app:checkDebugAarMetadata`, `AndroidManifest.xml` merger validation, and XML resource linking, hiding `compileSdk` and manifest defects.
  - Never downgrade library versions in response to an initial unresolved reference; match `compileSdk` to the library's AAR metadata floor (`compileSdk >= 35` for Wear Compose M3 `1.6.x`; `compileSdk >= 37` for `1.7.x` and Glance Wear widgets).

---

## 2. Wear OS Compose Material 3 (`androidx.wear.compose.material3`)

- **Do Not Use Legacy Horologist or `ScalingLazyColumn`**:
  - Use **`androidx.wear.compose.material3`** (`AppScaffold`, `ScreenScaffold`, `TransformingLazyColumn`, `ListHeader`, `Button`, `FilledTonalButton`, `Card`, `TitleCard`, `EdgeButton`, `ConfirmationDialog`, `CircularProgressIndicator`, `TimeText`).
  - Replace legacy `ScalingLazyColumn` with `TransformingLazyColumn`, Horologist scaffolds with M3 `AppScaffold` / `ScreenScaffold`, and `Chip` with `Button`.
- **Canonical `AppScaffold` -> `ScreenScaffold` -> `TransformingLazyColumn` Hierarchy**:
  - Declare exactly **one** `AppScaffold` at the Activity/root level (keeping `TimeText` persistent across navigation transitions) and one `ScreenScaffold` per screen.
  - Always pass `ScreenScaffold`'s `contentPadding` parameter into `TransformingLazyColumn` and apply `SurfaceTransformation` + `Modifier.transformedHeight` to items inside the list so cards and buttons morph smoothly along the circular watch bezel:
    ```kotlin
    val columnState = rememberTransformingLazyColumnState()
    val transformationSpec = rememberTransformationSpec()
    ScreenScaffold(
        scrollState = columnState,
        edgeButton = {
            EdgeButton(
                onClick = onPrimaryAction,
                modifier = Modifier.scrollable(
                    state = columnState,
                    orientation = Orientation.Vertical,
                    reverseDirection = true,
                    overscrollEffect = rememberOverscrollEffect(),
                ),
            ) { Text("Action") }
        },
    ) { contentPadding ->
        TransformingLazyColumn(
            state = columnState,
            contentPadding = contentPadding,
            modifier = Modifier.fillMaxSize(),
        ) {
            item {
                ListHeader(
                    modifier = Modifier
                        .fillMaxWidth()
                        .transformedHeight(this, transformationSpec)
                        .minimumVerticalContentPadding(ListHeaderDefaults.minimumTopListContentPadding),
                    transformation = SurfaceTransformation(transformationSpec),
                ) { Text("Controls") }
            }
            item {
                Button(
                    onClick = onToggle,
                    modifier = Modifier
                        .fillMaxWidth()
                        .transformedHeight(this, transformationSpec)
                        .minimumVerticalContentPadding(ButtonDefaults.minimumVerticalListContentPadding),
                    transformation = SurfaceTransformation(transformationSpec),
                ) { Text("Garage Door", maxLines = 1, overflow = TextOverflow.Ellipsis) }
            }
        }
    }
    ```
  - **Never** place `EdgeButton` as the final item inside `TransformingLazyColumn`; always pass it to the `edgeButton = { ... }` slot of `ScreenScaffold`.
  - Use `Column` directly inside `ScreenScaffold` **only** when the screen is strictly non-scrolling even at the largest system font scale (`@WearPreviewFontScales`).
  - Never hardcode text sizes or colors; always use `MaterialTheme.typography`, `MaterialTheme.colorScheme`, and `*Defaults` (`ButtonDefaults`, `CardDefaults`).
  - Use `LocalAmbientModeManager` (`rememberAmbientModeManager()`) instead of deprecated `AmbientLifecycleObserver`.

---

## 3. Wear OS 5+ One-Handed Double Pinch Gestures

- **Primary Action Gesture Support**:
  - On Wear Compose M3 `1.7.0+` (`androidx.wear.compose.material3.onehandedgesture`), wire primary hands-free actions (such as Push-to-Talk or primary confirmation) using `rememberOneHandedGestureConfiguration(action = OneHandedGestureAction.Primary, gestureId = "stable_id")`, a shared `MutableInteractionSource`, and `Modifier.oneHandedGesture(...)`.
  - **CRITICAL Placement Rule**: Place `OneHandedGestureClickIndicator(gestureConfig, indicatorState) { ... }` **inside** the button's `content` slot wrapping the inner `Icon`/`Text`—**NEVER** around the outer `Button` or `IconButton` (wrapping the outer button hides its background and border during the gesture hint animation).
  - Guard `indicatorState.showIndicator()` in `onGestureAvailable` with a remembered boolean flag (reset on `Lifecycle.Event.ON_STOP`) so state recompositions (e.g. toggling `"Talk"` / `"Send"`) do not replay the hint animation loop.
  - Also support hardware keyboard/rotary/stem focus triggers (`Modifier.focusable()`, `onKeyEvent`) for custom watch gesture bindings.

---

## 4. Wear OS Tiles & Glance Wear Widgets

- **Two Supported Surface Paths**:
  1. **Wear OS Tiles (`androidx.wear.tiles.TileService` + ProtoLayout Material3 `androidx.wear.protolayout:protolayout-material3`)**: Use for fast-swiping watch carousel Tiles on Wear OS 4/5 (`BIND_TILE_PROVIDER`, `ActionBuilders.LaunchAction` / `LoadAction` with explicit `Clickable` IDs, `TileService.getUpdater(context).requestUpdate(...)`).
  2. **Glance Wear Widgets (`androidx.glance.wear:wear` + Remote Compose `androidx.compose.remote`)**: When targeting `GlanceWearWidget` + `@AssociateWithGlanceWearWidget` `GlanceWearWidgetService`, return `WearWidgetDocument` using `@RemoteComposable` primitives (`RemoteBox`, `RemoteColumn`, `RemoteRow`, `RemoteText`, `RemoteButton`), convert primitives with `.rs`/`.rc`/`.rdp`/`.ri`, mutate state via `rememberMutableRemoteInt` + `valueChange` (never `mutableStateOf`), and wrap click intents in a lambda: `pendingIntentAction { ctx -> pendingIntent }`. Never use mobile `GlanceAppWidget` or `AppWidgetProvider` on Wear OS.

---

## 5. NFC Host Card Emulation (`HostApduService`) & Hardware Crypto (CSA Aliro)

- **Sub-100ms Screen-Off HCE Invariants**:
  - Declare `<host-apdu-service>` in `AndroidManifest.xml` with `android:exported="true"` and `android:permission="android.permission.BIND_NFC_SERVICE"`.
  - In `@xml/apduservice.xml`, set `android:requireDeviceUnlock="false"` and `android:requireDeviceScreenOn="false"` under `<host-apdu-service>` with the exact `<aid-group android:category="other">` AIDs (`A000000909ACCE5501`, `A000000909ACCE5502`).
  - **Zero Blocking Work in `processCommandApdu`**: NFC reader fields time out in ~100–150ms. Never perform disk I/O, SharedPreferences reads, network calls, or keypair generation inside `processCommandApdu`.
  - **Pre-Warmed Crypto & Ephemeral Key Pool**: Load the endpoint identity keypair and reader group public key into memory at service startup (`onCreate` / singleton init) and pre-generate the next ephemeral P-256 ECDH keypair asynchronously after each transaction so `processCommandApdu` executes strictly in-memory P-256 ECDH, HKDF-SHA256, and AES-128-CCM / ECDSA signing in `< 25ms`.
  - Trigger a crisp double-pulse haptic confirmation (`VibrationEffect.createWaveform(longArrayOf(0, 45, 60, 75), intArrayOf(0, 255, 0, 255), -1)`) on successful authentication response without blocking the APDU return byte array.

---

## 6. Jetpack Compose Performance & Kotlin Coroutines/Flow Discipline

- **Compose Stability & Recomposition (`jetpack-compose-performance`)**:
  - Under Strong Skipping Mode, flat `data class`es with `val` primitives/Strings are already inferred stable. **Never** annotate models containing `List`, `Set`, or `Map` with `@Immutable` or `@Stable` (forcing `O(N)` `.equals()` on every recomposition instead of `O(1)` instance equality `===`).
  - **Prohibit Backwards Writes**: Never mutate state after reading it in the same composition pass (`Read -> Write`). Never execute side effects directly in a `@Composable` body; use `LaunchedEffect`, `SideEffect`, or event callbacks.
  - **Phase Deferral**: Wrap animated or fast-changing offsets/alpha/scale in lambda modifiers (`Modifier.graphicsLayer { alpha = ... }`, `Modifier.offset { IntOffset(...) }`) to skip Composition and evaluate directly in Layout/Draw.
  - **Lazy Lists**: Always provide both a primitive `key` and `contentType` on `items(...)` in lazy lists. Use `collectAsStateWithLifecycle()` for Android UI Flows.
- **Coroutines, `StateFlow` & Networking Footguns (`kotlin-footguns`)**:
  - **Never use `GlobalScope`**. Tie coroutines to `viewModelScope`, `lifecycleScope`, or an explicit service `CoroutineScope(SupervisorJob() + Dispatchers.Default)` cancelled in `onDestroy()`.
  - Offload all HTTP, audio recording (`AudioRecord`), audio playback (`AudioTrack`), and keystore operations to `withContext(Dispatchers.IO)` or `Dispatchers.Default`—never block the main UI thread.
  - **Wear OS Bluetooth Proxy Networking**: Standard HTTPS calls via `OkHttpClient` automatically route over the paired phone's 5G connection via Wear OS `ConnectivityManager` Bluetooth proxy as long as the watch app requests `android.permission.INTERNET` and does not forcibly bind to a `TRANSPORT_WIFI`-only `NetworkRequest`. Configure explicit connect/read/write timeouts and exponential retry backoff.
  - **Hardware Request Signing (`AndroidKeyStore`)**: Generate non-exportable `KeyProperties.KEY_ALGORITHM_EC` (`secp256r1`) authentication keys in `AndroidKeyStore` (requesting `setIsStrongBoxBacked(true)` with graceful fallback to TEE when StrongBox is unavailable on the wearable SoC) and sign canonical request payloads (`timestamp || method || path || sha256(body)`) with `SHA256withECDSA`.
  - **Component & `PendingIntent` Security**: Explicitly set `android:exported="false"` on all internal activities, services, and receivers. Always pass `PendingIntent.FLAG_IMMUTABLE` (or pin an explicit `ComponentName` when `FLAG_MUTABLE` is required).
