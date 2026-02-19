# Expense Tracker

Simple Flask + SQLite expense tracking app with authentication, expenses CRUD, categories, monthly dashboard, CSV export, and tests.

## Features
- User registration and login
- Add/edit/delete expenses
- Category management
- Monthly summary dashboard
- CSV export
- CSV import with mapping UI (supports CIBC headerless format)
- Sample data generator
- Deployment-ready Procfile for Render/Railway

## Quickstart
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```
Open http://127.0.0.1:5000

## Database setup
The app automatically uses SQLite at `instance/expense_tracker.sqlite`.

To reset/init manually:
```bash
flask --app app init-db
```

## CSV import
Use **Import CSV** in the app navigation to upload transactions.

Supported formats:
- Header-based bank CSVs with inferred mapping for date, amount, debit, credit, description, and category.
- Headerless CIBC-style CSV rows where:
  - Column 1 = `YYYY-MM-DD` date
  - Column 2 = description
  - Column 3 = debit (money out)
  - Column 4 = credit (money in)
  - Additional columns are ignored

Debit/credit sign rule:
- If debit is present and numeric, imported amount is negative (`-debit`).
- Else if credit is present and numeric, imported amount is positive (`+credit`).
- Else the row is skipped.

The importer also deduplicates by `(user_id, date, normalized description, signed amount)`.

## Generate sample data
```bash
python generate_sample_data.py
```
Login with:
- Username: `demo`
- Password: `demo123`

## Run tests
```bash
pytest
```

## Deployment
- Uses `Procfile` with `gunicorn app:app`
- Works with Render or Railway Python services.
