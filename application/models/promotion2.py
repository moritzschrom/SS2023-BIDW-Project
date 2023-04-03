from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from application.database import Base


class Promotion2(Base):
    __tablename__ = "d_Promotion2"

    Promotion2ID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    IsPromotion: Mapped[bool] = mapped_column()
    SinceWeekYear: Mapped[str] = mapped_column(String(50), nullable=True)
    Interval: Mapped[str] = mapped_column(String(50), nullable=True)
