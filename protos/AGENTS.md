# Protobuf Guidelines

Also see [root AGENTS.md](../AGENTS.md) for cross-language standards.

## Compatibility

- All changes must be backwards compatible. Never re-use or change field numbers of existing fields.

## Schema Design

- Annotate fields with `optional` only when truly optional; omit it for required fields — `optional` maps to nullable types (e.g., `Option<T>` in Rust), bare fields map to non-nullable `T`.
- Use structured message types (e.g., `BasePath`) instead of plain scalars, and scope fields to operation-specific messages (e.g., `InsertTransaction`) rather than generic top-level ones.
- Don't duplicate data across messages — store each fact once and derive relationships. Prefer parallel sequences over maps when keys already exist in another field.

## Documentation

- Document the semantic meaning of both present and absent states for `optional` fields — explain when each case applies.
- Use precise domain terminology in field descriptions — avoid ambiguous abbreviations or terms that collide with domain concepts.
