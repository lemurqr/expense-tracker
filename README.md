# Expense Tracker

Simple Flask + SQLite expense tracking app with authentication, expenses CRUD, categories, monthly dashboard, CSV export, and tests.

## Features
- User registration and login
- Add/edit/delete expenses
- Category management with normalized category structure
- Monthly summary dashboard
- CSV export
- CSV import with mapping UI (supports CIBC headerless format)
- Internal signed amount model (`-` debit / `+` credit)
- Vendor capture + vendor-derived fallback for better auto-categorization
- Auto-categorization from category aliases + merchant keywords
- Transfer detection and exclusion from shared spending totals
- Personal expense detection and exclusion from shared pool
- Tag foundations (`David`, `Denys`, `Cookie`) stored on expenses
- Sample data generator
- Deployment-ready Procfile for Render/Railway

## Category system
The app now uses normalized categories:

- **Food & Dining**: Groceries, Restaurants, Bakery & Coffee
- **Housing & Home**: Mortgage, Condo Fees, Property Tax, Utilities, Home Maintenance & Repairs, Furniture & Appliances
- **Transportation**: Gas & Fuel, Car Maintenance & Registration, Insurance, Parking, Public Transit
- **Children**: School & Education, Sports & Activities, Camps & Lessons, Equipment
- **Pets**: Pet Food & Care
- **Lifestyle & Entertainment**: Entertainment, Subscriptions, Activities & Recreation, Tickets & Events
- **Shopping & Personal Items**: General Shopping, Electronics, Cosmetics & Personal Care, Clothing
- **Health & Wellness**: Pharmacy & Medical, Dentist & Dental
- **Social & Gifts**: Alcohol & Wine, Gifts & Presents
- **Travel & Vacation**: Travel & Vacation
- **Personal**: excluded from shared pool calculations
- **Transfers & Payments**: Credit Card Payments, Transfers

Legacy category names are mapped into this structure during import and login-time migration.

## Signed amount + vendor model
- Transactions are stored as a single signed `amount` value internally.
- Debit/credit imports are normalized as: debit -> negative, credit -> positive.
- Each transaction also stores `vendor` (nullable) and `paid_by` (`DK`/`YZ`, default `DK`).
- Vendor improves matching stability compared with noisy full descriptions.

## Spending logic
- **Transfers** are marked as non-spending and excluded from spending totals/summary charts.
- **Personal** expenses are excluded from shared pool totals.
- **Total spending** still includes Personal expenses (but excludes Transfers).
- **Refunds** are not moved to a refund category; when a category is present, refunds stay in that original category.

## Auto-categorization rules
Import and manual entry use keyword-based auto-categorization when category is missing.

Examples:
- Bakery & Coffee: `cafe`/`café`, `coffee`, `starbucks`, `tim hortons`, `boulangerie`, `bakery`, `patisserie`
- Groceries: `metro`, `iga`, `provigo`, `loblaws`, `super c`, `costco` (when Groceries exists)
- Gas & Fuel: `gas`, `esso`, `shell`, `petro`
- Public Transit: `stm`
- General Shopping: `amazon`, `shop`, `walmart`, `canadian tire`, `ikea` (unless Furniture & Appliances exists)
- Utilities: `hydro`, `bell`, `videotron`, `virgin`
- Sports & Activities: `hockey`, `tennis`, `ski`, `camp`, `piano`
- Subscriptions: `apple.com/bill`, `apple bill`, `itunes`, `icloud`, `apple music`, `apple tv`, `netflix`, `disney`, `spotify`

Special handling:
- Matching is case-insensitive and accent-insensitive (for example, `Café` matches `cafe`).
- `apple store` / `apple online store` are treated as **Electronics** (or **General Shopping** fallback if Electronics does not exist).
- `payment` and `payment thank you` are classified as **Credit Card Payments** (transfer/non-spending).

Transfer detection keywords include: `payment thank you`, `payment received`, `credit card payment`, `transfer`, `e-transfer`, `direct deposit`, `refund`, `return`, `points`.

Personal auto-detect keywords include: `salon`, `spa`, `barber`, `gym`, `hobby`, `massage`, `openai`, `open ai`, `chatgpt`.


## Learning Rules
The app includes a per-user learning system that remembers category corrections and reuses them in future imports and categorization.

How it works:
- Transfer detection always runs first and is never learned as a spending rule.
- Vendor-first pipeline order: learned vendor -> learned description -> vendor keywords -> description keywords -> optional AI fallback (disabled by default).
- Descriptions/vendors are normalized to lowercase, accent-insensitive text, punctuation removed, and whitespace collapsed.
- A pattern key is extracted (special patterns like `apple.com/bill`, otherwise first words).
- Transfer/payment-like transactions are excluded from learning.
- Generic stoplisted patterns (for example `shop`, `payment`, `service`, `transaction`) are not learned.
- Rules are applied after transfer detection and before keyword heuristics.

Where learning comes from:
- Editing a transaction category in the expense edit flow (`source=manual_edit`).
- Overriding a suggested category in CSV import preview (`source=import_override`).

Managing rules:
- Open **Rules** in the navigation (`/rules`) to view key type (Vendor/Description), pattern, category, hits, last use, and source, and update/delete/disable rules.

Resetting rules:
- Delete rows from `category_rules` in SQLite, or remove the DB file for a full reset.

Configuration:
- `ENABLE_LEARNING_RULES` Flask setting (default `True`).

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

Encoding support:
- The importer tries `utf-8-sig`, `utf-8`, `cp1252` (Windows-1252), then `latin-1`.
- If decoding fails, re-save the file as **CSV UTF-8** and upload again.

Supported formats:
- Header-based bank CSVs with inferred mapping for date, amount, debit, credit, description, vendor, and category.
- Headerless CIBC-style CSV rows where:
  - Column 1 = `YYYY-MM-DD` date
  - Column 2 = description
  - Column 3 = debit (money out)
  - Column 4 = credit (money in)
  - Additional columns are ignored

Vendor mapping/derivation:
- Optional vendor mapping supports headers like `vendor`, `merchant`, `payee`, `name`, `merchant name`.
- If vendor is not mapped, vendor is derived from description (noise tokens and trailing reference numbers removed).

Debit/credit sign rule:
- If debit is present and numeric, imported amount is negative (`-debit`).
- Else if credit is present and numeric, imported amount is positive (`+credit`).
- Else the row is skipped.

The importer deduplicates by `(user_id, date, normalized description, signed amount)`.

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
