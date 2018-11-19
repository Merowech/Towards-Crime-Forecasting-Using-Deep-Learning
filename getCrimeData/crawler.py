"""
Crawler Main Module.

Crawls crime data from various public cities repositories.
"""

__author__ = 'Udo Schlegel'

import pandas as pd
from sqlalchemy import create_engine, exists
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

chicago_data = {"db_name": "chicago_crimes",
                "url":
                "https://data.cityofchicago.org/resource/6zsd-86xi.json",
                "descriptor": "id"}
sanfran_data = {"db_name": "sanfran_crimes",
                "url": "https://data.sfgov.org/resource/cuks-n6tp.json",
                "descriptor": "pdid"}

sort_order = "DESC"

cities_to_crawl = [chicago_data, sanfran_data]

engine = create_engine(
    "",  # database connection
    connect_args={'sslmode': 'require'}
)
Base = declarative_base(engine)


def get_table(table_name):
    """
    Get table meta data.

    Args:
        table_name - Name of the table to insert the data
    """
    class Crimes(Base):
        """
        Crimes Meta Class to autoload.

        Database table will be loaded into.
        """

        __tablename__ = table_name
        __table_args__ = {"autoload": True}

    return Crimes


def loadSession():
    """
    Load the database session.

    Return:
        session of the session maker
    """
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


if __name__ == "__main__":

    for city in cities_to_crawl:
        print("Crawling for: " + city["db_name"])

        session = loadSession()

        Crimes = get_table(city["db_name"])

        limit = 10000
        offset = 0
        batch_not_in_db = 0

        new_entries = 0

        while batch_not_in_db < 4:
            batch_new = 0

            query = (city["url"] +
                     "?$limit=" + str(limit) + "&$order=" +
                     city["descriptor"] + "%20" + sort_order)
            if offset > 0:
                query += "&$offset=" + str(offset)
            print(query)
            raw_data = pd.read_json(query)

            def time_convert(x):
                """Convert time to time class."""
                return x.strftime('%Y-%m-%d %H:%M:%S')
            raw_data['date'] = raw_data['date'].apply(time_convert)

            if "location" in raw_data.columns:
                raw_data = raw_data.drop('location', 1)

            for index, row in raw_data.iterrows():
                # Check if entry is already in database table
                value = int(raw_data.at[index, city["descriptor"]])
                where = getattr(Crimes, city["descriptor"]) == value
                ret = session.query(exists().where(where)).scalar()
                if not ret:
                    # If not insert into table
                    raw_data.iloc[index:index+1].to_sql(city["db_name"],
                                                        engine,
                                                        if_exists="append")
                    new_entries += 1
                    batch_new += 1
                    batch_not_in_db = 0

            if batch_new == 0:
                batch_not_in_db += 1
            offset += limit

        print("Added " + str(new_entries) + " new entries")
