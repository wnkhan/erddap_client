# ERDDAP Client

This repo is organized for two parallel workflows:

- exploratory notebook work against NOAA-facilitated and related ERDDAP servers
- reusable Python helpers that can be carried into later projects

## Layout

- `notebooks/` contains exploratory work and prototypes
- `src/erddap_client/` contains reusable helpers extracted from notebooks
- `notes/` contains research notes, tool comparisons, and project ideas
- `data/raw/` is for downloaded responses kept in original form
- `data/interim/` is for cleaned subsets used during analysis
- `scripts/` is for small repeatable utilities once notebook experiments stabilize

## Current focus

The first notebook is:

- `notebooks/10_noaa_facilitated_erddap/glider_erddap_analysis.ipynb`

Current exploration goals:

1. Use glider datasets from `https://gliders.ioos.us/erddap/`.
2. Find gliders with similar temperature and salinity readings in other regions.
3. Graph tracks and related outputs using maps and `matplotlib`.
4. Build line plots for temperature, salinity, and derived sound speed across time and depth slices.

## Setup

Create an environment and install the project in editable mode:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .[dev]
python -m ipykernel install --user --name erddap-client --display-name "erddap-client"
```

Use the `erddap-client` kernel in Jupyter. That keeps notebook imports stable as helpers move from notebooks into `src/`.
