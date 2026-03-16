Build and maintain a simple household expense tracking web application.

## Stack
- Python
- Flask
- Postgres
- Docker Compose

## Core features
- Simple authentication
- Add / edit / delete expenses
- Categories and subcategories
- Vendor and optional description
- Monthly / filtered dashboard
- CSV import and export
- Household-aware tracking
- Shared expense and settlement support

## Interface
- Clean and simple UI
- Mobile-friendly where practical
- Compact, efficient workflows preferred over heavy UI complexity

## Data
Store:
- date
- amount
- vendor
- optional description
- category
- optional subcategory
- paid by
- source / confidence where applicable

## Quality
- Include focused tests for changed behavior
- Use the dedicated test DB only
- Preserve current app behavior unless intentionally changed

## Deployment / local run
Primary local workflow uses Docker Compose.

Typical rebuild:
- git pull
- docker compose up -d --build --force-recreate
