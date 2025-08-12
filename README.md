# Auction GM — Streamlit App (v7-aligned)

This is the no-install, free front end for your Auction GM Google Sheet (v7).

## Quick deploy
1) Upload these files to a GitHub repo.
2) Deploy on Streamlit Cloud (main file: streamlit_app.py).
3) In Streamlit → ⋮ → Edit secrets, set:
   SHEET_ID = "YOUR_GOOGLE_SHEET_ID"
   SLEEPER_LEAGUE_ID = "1253599128940707840"
   GOOGLE_SERVICE_ACCOUNT_JSON = """{ your full JSON here }"""
4) Share your Sheet with the service account email (Editor).

Then reload the app and use Archive & Reset (Practice Mode off).
