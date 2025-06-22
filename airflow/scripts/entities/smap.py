from sqlalchemy import Column, Date, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class SMAP(Base):
    __tablename__ = "smap"

    location = Column(Geometry('POINT', srid=4326), nullable=False)
    date = Column(Date, nullable=False)
    soil_moisture = Column(DOUBLE_PRECISION, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('date', 'location'),
    )
