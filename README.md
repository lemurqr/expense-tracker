# Expense Tracker

Simple Flask + SQLite expense tracking app with authentication, expenses CRUD, categories, monthly dashboard, CSV export, and tests.

## Features
- User registration and login
- Add/edit/delete expenses
- Category management
- Monthly summary dashboard
- CSV export
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
