from typing import Optional

from sqlalchemy import String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from application.database import Base, db_session
from application.models.promotion2 import Promotion2
from application.models.competition import Competition


class Store(Base):
    __tablename__ = "d_Store"

    StoreID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    Promotion2ID: Mapped[int] = mapped_column(ForeignKey("d_Promotion2.Promotion2ID"))
    CompetitionID: Mapped[int] = mapped_column(ForeignKey("d_Competition.CompetitionID"), nullable=True)
    StoreType: Mapped[str] = mapped_column(String(1))
    Assortment: Mapped[str] = mapped_column(String(1))
    StoreNr: Mapped[int] = mapped_column()

    Promotion2: Mapped["Promotion2"] = relationship()
    Competition: Mapped["Competition"] = relationship()

    @classmethod
    def find_by_business_key(cls, store_nr: str) -> Optional["Store"]:
        return db_session.query(cls).filter(cls.StoreNr == store_nr).first()
