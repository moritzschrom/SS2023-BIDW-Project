from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column

from application.database import Base


class Date(Base):
    __tablename__ = "d_Date"

    DateID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    Date: Mapped[datetime] = mapped_column()
    Day: Mapped[int] = mapped_column()
    Month: Mapped[int] = mapped_column()
    Year: Mapped[int] = mapped_column()
    Week: Mapped[int] = mapped_column()
    Quarter: Mapped[int] = mapped_column()
    DayOfWeek: Mapped[int] = mapped_column()
