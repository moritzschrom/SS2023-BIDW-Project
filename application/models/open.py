from sqlalchemy.orm import Mapped, mapped_column

from application.database import Base


class Open(Base):
    __tablename__ = "d_Open"

    OpenID: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    IsOpen: Mapped[bool] = mapped_column()
