from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from application.database import Base


class _StagingStore(Base):
    __abstract__ = True

    Store: Mapped[int] = mapped_column(primary_key=True)
    StoreType: Mapped[str] = mapped_column(String(1))
    Assortment: Mapped[str] = mapped_column(String(1))
    CompetitionDistance: Mapped[int] = mapped_column(nullable=True)
    CompetitionOpenSinceMonth: Mapped[int] = mapped_column(nullable=True)
    CompetitionOpenSinceYear: Mapped[int] = mapped_column(nullable=True)
    Promo2: Mapped[bool] = mapped_column()
    Promo2SinceWeek: Mapped[int] = mapped_column(nullable=True)
    Promo2SinceYear: Mapped[int] = mapped_column(nullable=True)
    PromoInterval: Mapped[str] = mapped_column(nullable=True)


class StagingStore(_StagingStore):
    __tablename__ = "sta_store"


class StagingStorePrev(_StagingStore):
    __tablename__ = "sta_store_prev"


class StagingStoreCur(_StagingStore):
    __tablename__ = "sta_store_cur"
