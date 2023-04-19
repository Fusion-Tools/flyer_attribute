# %%
import os
from dotenv import load_dotenv
from fusion_db import FusionDB

# %%
load_dotenv()
DB_USER = os.getenv("db_username")
DB_PASSWORD = os.getenv("db_password")

# %%
fdb = FusionDB(user=DB_USER, password=DB_PASSWORD, role=None)
# %%
