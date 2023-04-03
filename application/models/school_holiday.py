from sqlalchemy.orm import Mapped, mapped_column

from application.database import Base


class SchoolHoliday(Base):
    __tablename__ = "d_SchoolHoliday"

    SchoolHolidayID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    IsSchoolHoliday: Mapped[bool] = mapped_column()
