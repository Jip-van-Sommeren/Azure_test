import random
import string
import pandas as pd
from datetime import datetime, timedelta


def generate_random_date(start_year=1965, end_year=1995):
    start_date = datetime(year=start_year, month=1, day=1)
    end_date = datetime(year=end_year, month=12, day=31)
    time_between_dates = end_date - start_date
    random_number_of_days = random.randrange(time_between_dates.days)
    random_date = start_date + timedelta(days=random_number_of_days)
    return random_date


def generate_dutch_zipcode():
    numbers = "".join(random.choices(string.digits, k=4))
    letters = "".join(random.choices(string.ascii_uppercase, k=2))
    zipcode = f"{numbers} {letters}"

    return zipcode


def generate_phonenumber():
    numbers = "".join(random.choices(string.digits, k=8))
    return "+316" + numbers


def generate_random_number():
    return str(random.randint(1, 200))


def read_names(filename):
    df = pd.read_csv(filename)
    email_lst = []
    for i, j in zip(df["Firstname"], df["Lastname"]):
        email_lst.append(f"{i}.{j}@example.com")

    return email_lst[:200]


def read_street_names(filename):
    street_names = []
    with open(filename, "r") as file:
        for line in file:
            street_name = line.split(". ", 1)[1].strip()
            street_names.append(street_name)
    return street_names


def city_name(filename):
    city_names = []
    with open(filename, "r") as file:
        for line in file:
            city_names.append(line.strip())
    return city_names


# Example usage
filename = "/home/jip/Documents/azure_test/names/name_dataset/street/streetnames.txt"
filename_csv = "/home/jip/Documents/azure_test/names/name_dataset/data/NL.csv"


def main():
    df = pd.DataFrame()
    df["date_of_birth"] = [generate_random_date() for _ in range(200)]
    df["house_number"] = [generate_random_number() for _ in range(200)]
    df["zip_code"] = [generate_dutch_zipcode() for _ in range(200)]
    df["email"] = read_names(filename_csv)
    df["street"] = read_street_names(filename)
    df["country"] = "Netherlands"
    df["first_name"] = pd.read_csv(filename_csv)["Firstname"][:200]
    df["last_name"] = pd.read_csv(filename_csv)["Lastname"][:200]
    df["phone_number"] = [generate_phonenumber() for _ in range(200)]
    df["city"] = [
        city_name(
            filename="/home/jip/Documents/azure_test/names/name_dataset/city/cities.txt"
        )[random.randint(0, 174)]
        for _ in range(200)
    ]
    return df


if __name__ == "__main__":
    main()
