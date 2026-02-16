from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = "postgresql://postgres:Test%401234@localhost:5432/swing_db"

engine = create_engine(DATABASE_URL)