import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase

url = os.getenv("DATABASE_URL")
engine = create_engine(url)


class Base(DeclarativeBase):
    pass
