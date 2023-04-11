import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker, scoped_session

url = os.getenv("DATABASE_URL")
engine = create_engine(url)
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
db_session = scoped_session(Session)


class Base(DeclarativeBase):
    pass
