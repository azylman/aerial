---
name: superpowers
description: Use this skill for test-driven development (TDD), structured planning, root-cause debugging, code quality checks, and architectural guidelines when making code changes.
---

# Superpowers: Engineering Standards & Workflows

This skill defines the systematic software engineering standards Aerial must follow when writing, modifying, or refactoring code.

---

## 1. Test-Driven Development (TDD) Workflow

Always prioritize TDD whenever working on features, bug fixes, or refactoring:

1. **Write Failing Test(s) First**:
   - Write unit or integration tests that precisely capture the requested feature or reproduce the reported bug.
   - Run the test suite and verify that the test fails for the expected reason (Red).
2. **Minimal Implementation**:
   - Write the simplest code necessary to make the test pass (Green).
   - Avoid writing extraneous code or speculative features not covered by tests.
3. **Refactor & Verify**:
   - Clean up code structure, type safety, and readability while keeping all tests passing (Refactor).

---

## 2. Code Quality & Verification Protocols

Before declaring any coding task complete:

1. **Automated Verification**:
   - Run available linters, type checkers, and formatters (e.g., `go vet`, `eslint`, `ruff`, `prettier`, `clippy`).
   - Run the full test suite to verify no regressions were introduced.
2. **Diff & Hygiene Review**:
   - Inspect `git diff` to confirm changes are surgical and scoped strictly to the task.
   - Remove temporary debug logs, print statements, or leftover scratch files.
   - Preserve existing code comments, docstrings, and license headers.

---

## 3. Architecture & Design Standards

Maintain robust software architecture across all projects:

1. **Clean Separation of Concerns**:
   - Decouple domain/business logic from I/O, UI, network protocols, and external frameworks.
2. **Explicit Schemas & Type Safety**:
   - Use strict schemas and strongly typed data models (e.g., Go structs, TypeScript interfaces, Pydantic/Zod models) at I/O boundaries.
3. **Idiomatic & Defective-Free Code**:
   - Follow standard idiomatic practices for the language in use.
   - Handle errors explicitly and avoid silent failure swallowing.
