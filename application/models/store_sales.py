from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from application.database import Base
from application.models.store import Store
from application.models.date import Date
from application.models.open import Open
from application.models.promotion import Promotion
from application.models.school_holiday import SchoolHoliday


class StoreSales(Base):
    __tablename__ = "d_StoreSales"

    StoreID: Mapped[int] = mapped_column(ForeignKey("d_Store.StoreID"), primary_key=True)
    DateID: Mapped[int] = mapped_column(ForeignKey("d_Date.DateID"), primary_key=True)
    OpenID: Mapped[int] = mapped_column(ForeignKey("d_Open.OpenID"), primary_key=True)
    PromotionID: Mapped[int] = mapped_column(ForeignKey("d_Promotion.PromotionID"), primary_key=True)
    SchoolHolidayID: Mapped[int] = mapped_column(ForeignKey("d_SchoolHoliday.SchoolHolidayID"), primary_key=True)
    Sales: Mapped[int] = mapped_column()
    Customers: Mapped[int] = mapped_column()

    Store: Mapped["Store"] = relationship()
    Date: Mapped["Date"] = relationship()
    Open: Mapped["Open"] = relationship()
    Promotion: Mapped["Promotion"] = relationship()
    SchoolHoliday: Mapped["SchoolHoliday"] = relationship()
