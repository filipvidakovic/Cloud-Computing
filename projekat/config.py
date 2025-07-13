import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_PREFIX = os.getenv("PROJECT_PREFIX", "MyApp")