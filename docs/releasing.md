# Releasing

Dagmar's versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, a breaking change or a new feature raises the minor version (`0.10.0` → `0.11.0`), and a release of fixes only raises the patch version (`0.10.0` → `0.10.1`). [`CHANGELOG.md`](../CHANGELOG.md) records each version.

## Between releases

A pull request that changes what Dagmar does adds a line under `## [Unreleased]` in `CHANGELOG.md`, in the section that fits: `Added`, `Changed` (with **Breaking:** in front of a breaking change), `Removed` or `Fixed`, ending with a link to the pull request.

## Cutting a release

1. On a branch named for the release, for example `release-0.11.0`:
   - rename `## [Unreleased]` to `## [0.11.0] - <today's date>` and add an empty `## [Unreleased]` above it;
   - update the comparison links at the end of the file;
   - set the version in `package.json`: `pnpm version 0.11.0 --no-git-tag-version`.
2. Open a pull request with the title `chore(release): 0.11.0` and merge it once CI passes.
3. Tag the merge commit on `main` and push the tag:

   ```sh
   git switch main && git pull
   git tag -a v0.11.0 -m v0.11.0
   git push origin v0.11.0
   ```

The [release workflow](../.github/workflows/release.yml) then checks, tests and builds the tagged commit, checks that `package.json` has the tag's version, and publishes a GitHub Release with that version's changelog section as its notes.
