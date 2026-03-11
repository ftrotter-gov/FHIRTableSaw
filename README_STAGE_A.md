
This repo now includes the **Stage A profiler** that connects to a FHIR R4 server, samples data, and emits:

- `model-config.yaml`
- `table-schema.yaml` (standalone metadata sufficient to generate Postgres DDL)
- `ignore_extensions.yaml`

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip
pip install -e .[dev]
```

## Run

```bash
. .venv/bin/activate
python -m fhir_tablesaw.cli \
  --base-url https://hapi.fhir.org/baseR4 \
  --out-dir out \
  --max-resources-per-type 20 \
  --page-size 20
```

Outputs will be written to `out/`.

## Tests

```bash
. .venv/bin/activate
pytest -q
```

## Ignore extensions

`ignore_extensions.yaml` is a list of URL patterns (supports `*` wildcards) that will be ignored **everywhere** they appear.

The default seed includes:

```yaml
- http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-contactpoint-availabletime
```

## NDH carve-out: Organization subtyping

Organizations are split into independent subtype entities based on:

`Organization.type[].coding[].code`

If an Organization has codes `pay` and `fac`, it is profiled into both `organization_pay` and `organization_fac`.
If an Organization has no type, it stays in `organization`.
