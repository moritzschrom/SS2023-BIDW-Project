from typing import Optional

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from application.database import Base, db_session


class Competition(Base):
    __tablename__ = "d_Competition"

    CompetitionID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    CompetitionDistance: Mapped[int] = mapped_column(nullable=True)
    OpenSinceMonthYear: Mapped[str] = mapped_column(String(50), nullable=True)

    @classmethod
    def find_by_business_key(cls, competition_distance: int, open_since_month_year: str) -> Optional["Competition"]:
        return db_session.query(cls).filter(
            cls.CompetitionDistance == competition_distance,
            cls.OpenSinceMonthYear == open_since_month_year
        ).first()
