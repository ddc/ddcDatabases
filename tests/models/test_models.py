from sqlalchemy import Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ModelTest(Base):
    __tablename__ = "model_test"
    id: Mapped[int] = mapped_column(primary_key=True, unique=True, autoincrement=True)
    name: Mapped[str] = mapped_column(nullable=True, server_default="Test")
    enabled: Mapped[bool] = mapped_column(Boolean, server_default="1")
