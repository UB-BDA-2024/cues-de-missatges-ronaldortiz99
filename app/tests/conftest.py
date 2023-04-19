import os
os.environ["ENVIRONMENT"] = "test"

from app.settings import Settings

settings = Settings()