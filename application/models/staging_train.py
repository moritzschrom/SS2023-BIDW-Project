from datetime import date

from sqlalchemy.orm import Mapped, mapped_column

from application.database import Base


class _StagingTrain(Base):
    __abstract__ = True

    Id: Mapped[int] = mapped_column(primary_key=True)
    Store: Mapped[int] = mapped_column()
    DayOfWeek: Mapped[int] = mapped_column()
    Date: Mapped[date] = mapped_column()
    Sales: Mapped[int] = mapped_column()
    Customers: Mapped[int] = mapped_column()
    Open: Mapped[bool] = mapped_column()
    Promo: Mapped[bool] = mapped_column()
    StateHoliday: Mapped[bool] = mapped_column()
    SchoolHoliday: Mapped[bool] = mapped_column()


class StagingTrain(_StagingTrain):
    __tablename__ = "sta_train"


class StagingTrainPrev(_StagingTrain):
    __tablename__ = "sta_train_prev"


class StagingTrainCur(_StagingTrain):
    __tablename__ = "sta_train_cur"
