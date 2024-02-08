from datetime import datetime
from typing import Union, List, Optional

from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String,DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import urllib.parse

Base = declarative_base()


class LocationRating(Base):
    __tablename__ = "LocationRating"

    ID = Column(Integer, primary_key=True, index=True)
    Nuts2Code = Column(String(255))
    Country = Column(String(255))
    LocationName = Column(String(255))
    NumAccoms = Column(Float)
    NetOccupancyRate = Column(Float)
    ArrivalsAccommodation = Column(Float)
    ExpenditureAccomodation = Column(Float)
    ExpenditureTrip = Column(Float)
    HicpCountry = Column(Float)
    LastUpdated  = Column(DateTime, default=datetime.utcnow)


# Define the Pydantic model for request data
class LocationRatingCreate(BaseModel):
    nuts_2_code: Union[str, None] = Field(None, alias='Nuts2Code')
    country: Union[str, None] = Field(None, alias='Country')
    location_name: Union[str, None] = Field(None, alias='LocationName')
    num_accoms: Union[float, None] = Field(None, alias='NumAccoms')
    net_occupancy_rate: Union[float, None] = Field(None, alias='NetOccupancyRate')
    arrivals_accommodation: Union[float, None] = Field(None, alias='ArrivalsAccommodation')
    expenditure_accomodation: Union[float, None] = Field(None, alias='ExpenditureAccomodation')
    expenditure_trip: Union[float, None] = Field(None, alias='ExpenditureTrip')
    hicp_country: Union[float, None] = Field(None, alias='HicpCountry')
    last_updated: Optional[datetime] = Field(None, alias='LastUpdated')

    class Config:
        populate_by_name = True



azure_host = "yc2401data.mysql.database.azure.com"
azure_username = "yc2401"
azure_password = "abcd1234ABCD!@#$"
azure_database = "hotel"

# Construct Azure connection URL
SQLALCHEMY_DATABASE_URL = f"mysql+mysqlconnector://{azure_username}:{urllib.parse.quote_plus(azure_password)}@{azure_host}/{azure_database}"



engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)  # Create tables

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()