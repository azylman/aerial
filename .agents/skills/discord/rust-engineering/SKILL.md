---
name: rust-engineering
description: Use when writing, reviewing, refactoring, testing, porting, or debugging Rust (.rs) code, Cargo.toml workspaces, Tokio/Axum services, ONNX Runtime (ort), or FFI/CUDA crates across Aerial and sidecar repositories.
---

# Rust Engineering, Async Services & FFI Safety Standards

Synthesized from [`morganlinton/BeautifulRust`](https://github.com/morganlinton/BeautifulRust) (Google Rust Style Guide, Rust API Guidelines, Effective Rust, Rust Design Patterns, and Microsoft's Pragmatic Rust Guidelines) and Aerial's Rust sidecar architecture (`senses`, `pjev`).

---

## 1. Mindset & Core Principles

- **Write Rust-Shaped Code**: Model fallibility with `Result<T, E>`, absence with `Option<T>`, resource lifetimes with ownership and `Drop`, and closed state machines with data-carrying `enum`s. Avoid OOP/inheritance patterns (`Deref` to simulate base classes, `Manager`/`Helper`/`Util` suffixes, or sentinel values like `-1` / `""`).
- **Make Invalid States Unrepresentable**: Encode domain invariants in types—use newtypes (`struct SampleRateHz(u32)`), two-variant enums instead of `bool` parameters in public/shared signatures, and fallible constructors (`try_new`, `TryFrom`) that validate once at the boundary.
- **Application Code vs. Library Code**:
  - **Library / Shared Crates**: Follow strict API design rules—typed `thiserror` error enums/structs, minimal public visibility, `#[non_exhaustive]` on evolving enums/structs, zero leaked third-party types in public signatures, and iterator return types.
  - **Service / Application Binaries (`senses`, `pjev`)**: Favor directness and clarity—concrete types, `anyhow::Result` with `.context("...")` at orchestration boundaries, and explicit `axum` / `tokio` state structs wrapped in an internal `Arc<Inner>`.

---

## 2. Mandatory Pre-Delivery Verification

Before committing or submitting any Rust change, run and pass all checks with zero warnings:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

- **Fix Lints Instead of Silencing**: When a Clippy lint is genuinely a false positive on a specific item, use `#[expect(clippy::lint_name, reason = "concrete justification")]` on the narrowest scope (never `#![allow(...)]` or `#![deny(warnings)]` in source files). Configure workspace-wide lints in `Cargo.toml` under `[lints]` or `[workspace.lints]`.

---

## 3. Ownership, Borrowing & Zero Appeasement Clones

- **Do Not `.clone()` Just to Satisfy the Borrow Checker**: A clone added purely to silence a borrow error creates a silent copy whose mutations or memory footprint diverge. Instead:
  1. Move owned fields out of `&mut self` using `std::mem::take`, `std::mem::replace`, or `Option::take`.
  2. Split monolithic structs into cohesive sub-structs so the compiler can track disjoint field borrows across methods.
  3. Shorten borrow lifetimes with inner block scopes or named intermediates.
  4. When cloning an `Arc` or small immutable configuration intentionally, keep the `Arc` internal to a handle struct (`#[derive(Clone)] pub struct ServiceHandle(Arc<Inner>);`) or document why the clone is deliberate.
- **Accept the Most General Borrowed Type**:
  - Take `&str` (never `&String`), `&[T]` (never `&Vec<T>`), `&T` (never `&Box<T>` or `&Arc<T>`), and `impl AsRef<Path>` for read-only paths.
  - Take ownership (`T`) by value only when the function stores or consumes the argument—never take `&T` only to `.clone()` it internally.
- **Prefer Owned Data in Structs**: Avoid lifetime parameters (`&'a T`) on long-lived domain structs unless zero-copy parsing is benchmarked and required.

---

## 4. Error Handling & Panic Discipline

- **Zero `unwrap()` in Production Paths**:
  - Never use `.unwrap()` or `panic!` on fallible I/O, network requests, JSON parsing, channel sends/receives, or user inputs.
  - Use `?` propagation, `let Some(v) = opt else { return ... };`, or `if let`.
  - Reserve `.expect("invariant: <why this holds and relevant values>")` strictly for compile-time or startup invariants proved by construction.
- **Domain Errors (`thiserror`) vs. Application Errors (`anyhow`)**:
  - Define module and HTTP API errors with `thiserror::Error`, implementing `std::error::Error + Display + Debug + Send + Sync + 'static`. Map domain errors cleanly into HTTP status codes via `axum::response::IntoResponse`.
  - Keep `Display` messages lowercase, concise, and free of trailing punctuation (`"invalid audio sample rate: {rate}"`).
  - Destructors (`Drop`) must never panic or block; expose an explicit `async fn close(self) -> Result<()>` for fallible teardown.

---

## 5. Async (`tokio` / `axum`), Concurrency & CPU/GPU Offloading

- **`Send + Sync` Futures & Short Lock Scopes**:
  - Assert that service clients and state handles are `Send + Sync` at compile time.
  - **Never hold a `std::sync::MutexGuard` or `RwLockReadGuard` across an `.await` point.** Extract or clone the needed state inside a tight block scope, drop the guard, and then `.await`.
  - Prefer `std::sync::Mutex` / `parking_lot::Mutex` for fast in-memory state mutations (a few nanoseconds); use `tokio::sync::Mutex` or `RwLock` only when the lock genuinely must be held across `.await` points (such as serializing an async hardware session).
- **Mandatory Offloading for CPU, ONNX (`ort`), and Blocking FFI Work**:
  - Never run synchronous CPU-bound inference (ONNX Runtime `Session::run`, audio resampling, tokenization, GGUF/llama.cpp calls, or blocking filesystem I/O) directly on a `tokio` async worker thread—doing so starves the async reactor, stalls health checks (`/healthz`), and drops concurrent requests.
  - Offload heavy compute and FFI calls to `tokio::task::spawn_blocking` or a dedicated bounded worker thread pool communicating via `tokio::sync::mpsc` / `oneshot` channels.
- **Bounded Channels & Backpressure**:
  - Always use bounded channels (`tokio::sync::mpsc::channel(capacity)`) and explicit request timeouts (`tokio::time::timeout`) rather than unbounded queues that can exhaust memory under load.

---

## 6. Unsafe, FFI & CUDA Hygiene

- **Minimize & Isolate `unsafe`**:
  - Encapsulate FFI, raw pointers, or CUDA/ONNX buffer interop inside the smallest possible private module with a safe public Rust wrapper.
  - Every `unsafe { ... }` block and `unsafe impl` MUST be preceded by a `// SAFETY:` comment explaining why all preconditions hold.
  - Every `pub unsafe fn` MUST include a `# Safety` rustdoc section detailing the caller's obligations, and crates using `unsafe` should enable `#![deny(unsafe_op_in_unsafe_fn)]`.
  - Never use `std::mem::transmute` to bypass lifetimes or convert enums; use `#[repr(C)]`, `CStr`/`CString`, and RAII drop wrappers for C-allocated pointers.

---

## 7. Testing & Benchmarking

- Place unit tests for private internals in `#[cfg(test)] mod tests { use super::*; ... }` and public contract tests in `tests/`.
- Test observable behavior, round-trip invariants, and error branches (malformed inputs, cancelled contexts, channel closures)—never internal constants.
- Use temporary directories (`tempfile::tempdir()`) for filesystem tests; never mutate host paths outside `/tmp`.
- Include context in assertions (`assert_eq!(got, want, "for input {input:?}")`).
