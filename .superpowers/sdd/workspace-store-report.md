# Workspace store report

## Status

DONE

## TDD evidence

- RED: `pnpm --dir loxa-app test:unit -- src/stores/workspace-store.test.ts` exited 1 because Vite could not resolve the intentionally absent `./workspace-store`; the existing 462 unit tests passed.
- GREEN: `pnpm --dir loxa-app exec vitest run src/stores/workspace-store.test.ts` passed all 18 workspace-store cases.

## Implemented

- Added exactly pinned `zustand` `5.0.14` dependency and lockfile metadata.
- Added a typed workspace store for route and sidebar preferences only.
- Added exact width constants, clamped width actions, collapse/expand preservation, reset, derived effective-width selector, and atomic selectors.
- Added versioned persistence with an exact sidebar-only allowlist, migration fallback, runtime validation, and safe unavailable-storage behavior.
- Added coverage for defaults, route exclusion, persistence shape/version, valid rehydration, invalid storage data, forbidden state, and stable action references.

## Verification

- Focused unit test: 18 passed.
- Typecheck: passed.
- Lint: passed.
- Format check: passed.
- Production build: passed.

## Concerns

None.
