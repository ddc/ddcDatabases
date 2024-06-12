# -*- coding: utf-8 -*-
from sqlalchemy import Boolean
from sqlalchemy.orm import declarative_base, Mapped, mapped_column


Base = declarative_base()


class ModelTest(Base):
    __tablename__ = "model_test"
    id: Mapped[int] = mapped_column(primary_key=True, unique=True, autoincrement=True)
    name: Mapped[str] = mapped_column(nullable=True, server_default="Test")
    enable: Mapped[Boolean] = mapped_column(Boolean, server_default="1")
