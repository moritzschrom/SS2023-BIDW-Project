from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from application.database import Base


class Competition(Base):
    __tablename__ = "d_Competition"

    CompetitionID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    CompetitionDistance: Mapped[int] = mapped_column()
    OpenSinceMonthYear: Mapped[str] = mapped_column(String(50), nullable=True)
