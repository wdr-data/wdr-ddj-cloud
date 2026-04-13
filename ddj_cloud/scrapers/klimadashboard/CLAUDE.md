# Technology stack

- Python 3.11
- uv
- Datawrapper (Charts)
- SQLite Database (MaStR-Daten)
- Fraunhofer Energy Charts API (Energiemix)
- MaStR SOAP API (Windkraft-Ausbau)
- Sentry (Monitoring)

## Step 1: POC Datawrapper -- DONE

- Look at src/energiemix.py which is a crude sample of a gather-process-store-publish pipeline
- Look for errors and improve the code

### Findings & Fixes (2026-03-30)

7 bugs fixed in `src/energiemix.py`:
1. `MIX_NOTES` was defined twice, shadowing `POWER_NOTES` -> renamed second to `POWER_NOTES`
2. `fetch_public_power()` called wrong API endpoint (`SHARE_FORECAST` instead of `PUBLIC_POWER`)
3. `upload_to_datawrapper()` used undefined `DATAWRAPPER_CHART_ID` -> changed to `dw_id` param
4. Column selection used tuple syntax instead of list (`df["a", "b"]` -> `df[["a", "b"]]`)
5. `POWER_NOTES` was undefined because of bug #1
6. Raw DataFrame passed to `upload_to_datawrapper` instead of CSV -> added `build_csv_from_index()`
7. Returned CSV string but caller expected DataFrame -> now returns `df_combined`

## Step 2: PHP to Python -- DONE

- Look at the msr_php subfolder containing PHP scripts to scrape and process wind data
- Construct a Python version of it "msr_wind.py", analog to the src/energiemix.py
- Document in README_msr.md, noting all secrets and keys needed
- Suggest msr_solar.py for solar energy

### Findings (2026-03-30)

Created `src/msr_wind.py` porting `msr_php/wka_daily.php` + `msr_php/wka_to_data.php`:
- Uses SQLite instead of MySQL, requests instead of PHP SoapClient, pandas instead of per-row SQL
- `fetch_recent_units()`: fetches new/updated wind units from MaStR API
- `process_daily_data()`: calculates daily capacity (installed, planned, required for 2030 targets)
- Documented all secrets in `README_msr.md`
- Solar suggestion included in README_msr.md (energietraeger: "Solare Strahlungsenergie", 215 GW target)

## Step 3: Add monitoring -- DONE

- Look at the ../../utils to understand sentry
- Add useful sentry functions

### Findings (2026-03-30)

Added `sentry_sdk.capture_exception(e)` to all API calls in both files:
- `energiemix.py`: all 4 Fraunhofer API fetch functions
- `msr_wind.py`: SOAP API call + per-unit error handling (individual failures don't crash the run)