# Contributing

## Setup

You need [uv](https://docs.astral.sh/uv/).

```sh
git clone https://github.com/rickcrawford/tpuf-arrow && cd tpuf-arrow
uv sync
```

## Checks

```sh
uv run pytest -m "not live"                 # offline tests, a few seconds
uv run pytest -m "not live" --cov           # with coverage, which has to stay at 95% or higher
uv run ruff check . && uv run ruff format --check .
uv run pyright
```

CI runs the tests on Python 3.12 and 3.13, with both the newest and the oldest dependency versions that `pyproject.toml` allows. Lint and type checks run once, against the lockfile. To try the oldest versions locally:

```sh
uv run --resolution lowest-direct pytest -m "not live"
```

## Tests

The offline tests use a fake turbopuffer client in `tests/conftest.py`. It's strict on purpose: it serializes writes as JSON with NaN rejected, enforces the 256-row cap on embedding writes, returns 404s for namespaces that don't exist, and tracks how many requests are in flight. If you find something the real service does that the fake doesn't, add it to the fake and add a live test that pins it down.

The live tests (marked `live`) need `TURBOPUFFER_API_KEY` in the environment or in a `.env` at the repo root. They create namespaces named `tpuf-arrow-test-*`, delete them when the session ends, and cost a fraction of a cent.

```sh
echo 'TURBOPUFFER_API_KEY=tpuf_...' > .env  # gitignored
uv run pytest                               # everything
```

`tests/test_examples.py` runs each script in `examples/` against the sample, loaded once per test session. A new example needs a test there, and an offline test in `tests/test_sample.py` fails until it has one.

## Sample data

`data/` is checked in. `scripts/make_sample.py` regenerated it from a local warehouse of the full Shopping Queries dataset. You shouldn't need to run it, and it won't work without that warehouse. `data/README.md` explains how the sample was cut.

## Docs

The README links to images by absolute `raw.githubusercontent.com` URLs so they render on PyPI. The images live in `docs/images/` and switch colors for dark mode using CSS in the SVG.

## Artifacts

Every CI run uploads two artifacts: `dist` (the wheel, the sdist, and `SHA256SUMS`, built and installed into a clean environment first) and `coverage` (`coverage.xml`). Download them from the run's summary page to try a build before it's released.

## Releasing

1. Update the version in `pyproject.toml`, and change the `CHANGELOG.md` heading from `## 0.1.0 (unreleased)` to the date, like `## 0.1.0 (2026-10-24)`. The release fails if the heading still says unreleased.
2. Commit, then tag: `git tag v0.1.0 && git push origin v0.1.0`.
3. The release workflow runs lint, types, and tests without any token permissions. Then it builds the sdist and wheel, smoke-tests the wheel, and attests where they were built. It publishes to TestPyPI and then PyPI with trusted publishing, so no token is stored in the repo. Last, it creates a GitHub release with the files, `SHA256SUMS`, and that version's section of the changelog as the notes.

Before the first release, add a pending trusted publisher on both [PyPI](https://pypi.org/manage/account/publishing/) and [TestPyPI](https://test.pypi.org/manage/account/publishing/): project `tpuf-arrow`, owner `rickcrawford`, repository `tpuf-arrow`, workflow `release.yml`, and environment `pypi` or `testpypi`. In the GitHub repo settings, create the `pypi` environment with a required reviewer, so a tag can't publish without someone approving it.

The live tests are optional in CI. They run on pushes to main when the repo has a `TURBOPUFFER_API_KEY` secret. Without one, the live job passes and leaves a warning on the run saying the tests were skipped.
