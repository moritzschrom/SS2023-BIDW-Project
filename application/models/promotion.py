from sqlalchemy.orm import Mapped, mapped_column

from application.database import Base


class Promotion(Base):
    __tablename__ = "d_Promotion"

    PromotionID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    IsPromotion: Mapped[bool] = mapped_column()
