import os
from flask_appbuilder.security.manager import AUTH_DB

# Secret key for signing cookies
SECRET_KEY = os.environ.get("SECRET_KEY") or "airflow_webserver_secret_key"

# Authentication type
AUTH_TYPE = AUTH_DB

# Theme
#APP_THEME = "lightly.css"

# UI Settings
ENABLE_PROXY_FIX = True

# CORS settings
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

# Flask settings
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # Set to True if using HTTPS
PERMANENT_SESSION_LIFETIME = 43200  # 12 hours in seconds

# Feature flags
ENABLE_CHANGES = False
ENABLE_SWAGGERUI = True