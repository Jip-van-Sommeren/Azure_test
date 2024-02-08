from tables import LocationRating, LocationRatingCreate
from datetime import datetime
import pandas as pd
from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session, sessionmaker, declarative_base
from sqlalchemy import create_engine
import urllib.parse

# from endpoints.location_rating import create_location_rating
from dataset import main

azure_host = "yc2401hotel.mysql.database.azure.com"
azure_username = "yc2401"
azure_password = "abcd1234ABCD!@#$"
azure_database = "hoteldb"

# Construct Azure connection URL
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{azure_username}:{urllib.parse.quote_plus(azure_password)}@{azure_host}/{azure_database}"

# # Create a database engine
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Base = declarative_base()
# # Create a SessionLocal class
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# # Ensure your database tables are created
# Base.metadata.create_all(bind=engine)


def convert_nan_to_none(row):
    """Converts pandas nan to None (NULL in SQL)"""
    return {key: (None if pd.isnull(value) else value) for key, value in row.items()}


def read_csv_and_post_to_db(csv_path):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_path)

    # Create a database session
    db = SessionLocal()

    try:
        # Iterate through DataFrame rows
        for index, row in df.iterrows():
            # Create a LocationRating instance for each row
            # geo,num_of_accoms.csv,country,Net_occupancy_rate.csv,expenditure_accomodation.csv,
            # Arrivals_accommodation.csv,expenditure_per_trip_avg.csv,hicp_per_country.csv,score,location_str
            cleaned_row = convert_nan_to_none(row)
            db_location_rating = LocationRating(
                Nuts2Code=cleaned_row["geo"],
                Country=cleaned_row["country"],
                LocationName=cleaned_row["location_str"],
                NumAccoms=cleaned_row["num_of_accoms.csv"],
                NetOccupancyRate=cleaned_row["Net_occupancy_rate.csv"],
                ArrivalsAccommodation=cleaned_row["Arrivals_accommodation.csv"],
                ExpenditureAccomodation=cleaned_row["expenditure_accomodation.csv"],
                ExpenditureTrip=cleaned_row["expenditure_per_trip_avg.csv"],
                HicpCountry=cleaned_row["hicp_per_country.csv"],
                LastUpdated=datetime.utcnow(),  # or parse cleaned_row['last_updated'] if it exists
            )
            print(db_location_rating.ArrivalsAccommodation)
            # Add to the session and commit
            db.add(db_location_rating)

        # Commit the session once all rows are processed
        db.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        db.rollback()
    finally:
        db.close()


# if __name__ == "__main__":
#     csv_file_path = "/home/jip/Documents/hotel/bookings/location_data.csv"
#     read_csv_and_post_to_db(csv_file_path)\

csv_file_path = "/home/jip/Documents/hotel/bookings/location_data.csv"

# Read the CSV file into a pandas DataFrame
# df = pd.read_csv(csv_file_path)


# Function to manually handle the session for each operation
def process_row(row, db: Session):
    # Convert row to Pydantic model instance
    cleaned_row = convert_nan_to_none(row)
    location_rating_data = LocationRatingCreate(
        nuts_2_code=cleaned_row["geo"],
        country=cleaned_row["country"],
        location_name=cleaned_row["location_str"],
        num_accoms=cleaned_row["num_of_accoms.csv"],
        net_occupancy_rate=cleaned_row["Net_occupancy_rate.csv"],
        arrivals_accommodation=cleaned_row["Arrivals_accommodation.csv"],
        expenditure_accommodation=cleaned_row["expenditure_accomodation.csv"],
        expenditure_trip=cleaned_row["expenditure_per_trip_avg.csv"],
        hicp_country=cleaned_row["hicp_per_country.csv"],
        last_updated=pd.Timestamp("now"),
    )
    # Use the existing create function
    create_location_rating(location_rating=location_rating_data, db=db)


# Main function to process the CSV file
def process_csv(csv_path):
    db = Session(bind=engine)  # Create a session directly
    try:
        df = pd.read_csv(csv_path)
        for index, row in df.iterrows():
            print(row)
            # process_row(row, db)
    except Exception as e:
        print(f"An error occurred: {e}")
        db.rollback()
    finally:
        db.close()


# Insert DataFrame into SQL table
def insert_df_to_sql():
    df = main()
    print(df)
    df.to_sql("user", con=engine, if_exists="append", index=False)


if __name__ == "__main__":
    insert_df_to_sql()
