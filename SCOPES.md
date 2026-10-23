# Commit Scopes

Valid scopes for commits in this repository. A scope names the section of the code that changed. It is never the repository name: `kopper` says nothing, because every commit here touches kopper. The repo is small, so the list is short. Release commits made by the Maven release plugin have no scope.

## Modules

| Scope | When to use |
|---|---|
| `core` | The kopper core library: `kopper-core/` (`dyre/`, `serde/`) |
| `cast` | The cast module: `kopper-cast/` |

## Infrastructure

| Scope | When to use |
|---|---|
| `build` | Maven build, `pom.xml` (except dependency versions), `checkstyle/` |
| `deps` | Dependency version bumps |
| `ci` | GitHub Actions workflows: `.github/workflows/` |
| `release` | Version bumps and release notes |
| `docs` | `README.md`, `CONTRIBUTING.adoc`, `CODE_OF_CONDUCT.adoc` and other documentation |
| `claude` | AI context files (`CLAUDE.md`, `.claude/`) |

## Multiple contexts

Comma-separate when one commit intentionally touches two distinct sections:

```
feat(core,cast): Added a serde for the new cast type
```

Omit the scope only when the change is so sweeping that listing every scope would be noise, or when no section applies (for example a root `.gitignore`). For anything that touches code, name the section.
