# Daiso Beauty Data Project

This repository reorganizes the Daiso beauty project into a GitHub-friendly structure covering trust, product strategy, and logistics optimization.

## What This Repo Covers

- `src/acquisition`: Daiso mall crawling, OCR, and ingredient extraction modules
- `src/absa`: ABSA training, evaluation, and inference pipeline
- `src/trend`: Naver search trend and soft-landing analysis scripts
- `src/gis`: GIS analysis code for foreign demand zones and logistics optimization
- `src/bigquery`: BigQuery loading and ETL modules
- `notebooks/`: final analysis notebooks
- `docs/`: reports, ERD, derived-variable definitions, and storytelling references

## Business Impact

- **Risk defense**: OCR plus ingredient filtering to block risky products before launch
- **Product strategy**: SLI plus ABSA plus search trends to identify soft-landing products and target brands
- **Logistics optimization**: GIS-based Hub and Spoke inventory strategy

## Repository Layout

```text
src/
  acquisition/
  absa/
  trend/
  gis/
  bigquery/
  common/
notebooks/
  eda/
  advanced/
  gis/
docs/
  reports/
  project/
  storytelling/
data/
```

## Data Policy

Raw data and large generated artifacts are not included in GitHub.
See `data/README.md` and `docs/project/` for data notes and project documentation.

## Recommended Commit Order

1. `chore: initialize repository structure and gitignore`
2. `feat: add crawling and OCR ingestion modules`
3. `feat: add EDA notebooks and preprocessing workflow`
4. `feat: add ABSA pipeline and search trend analysis`
5. `feat: add GIS and BigQuery integration modules`
6. `docs: add reports and project documentation`
