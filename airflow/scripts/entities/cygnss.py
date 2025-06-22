from sqlalchemy import Column, Float, Date, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, SMALLINT
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()

class CYGNSS(Base):
    __tablename__ = "cygnss"

    location = Column(Geometry('POINT', srid=4326), nullable=False)
    date = Column(Date, nullable=False)
    reflectivity = Column(DOUBLE_PRECISION, nullable=False)
    incident_angle = Column(DOUBLE_PRECISION)
    antenna_gain = Column(DOUBLE_PRECISION)
    ddm_peak_delay = Column(SMALLINT)

    __table_args__ = (
        PrimaryKeyConstraint('date', 'location'),
    )
