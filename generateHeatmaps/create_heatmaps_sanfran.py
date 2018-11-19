"""
San Francisco Heatmap Generation Main Module.

Crawls crime data from database table and converts it to a heatmap.
"""

__author__ = 'Udo Schlegel'
import numpy as np
import pandas as pd

from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import UniqueConstraint

DEBUG = False

engine = create_engine(
    "",  # database connection
    connect_args={'sslmode': 'require'}
)

Base = declarative_base(engine)


def get_table(table_name):
    """Crawl the table information from the DB."""
    class Crimes(Base):
        """Class for the Crimes table."""

        __tablename__ = table_name
        __table_args__ = {"autoload": True}

    return Crimes


def loadSession():
    """Load database session to crawl table info."""
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def create_image_table(name):
    """Crawl the table information from the DB."""
    class CrimesImages(Base):
        """Class for the Crimes Images table."""

        __tablename__ = name

        id = Column(Integer, primary_key=True)
        image = Column(BYTEA)
        woy = Column(Integer)
        year = Column(Integer)
        crime = Column(String)

        __table_args__ = (UniqueConstraint('woy', 'year', 'crime'),)

    return CrimesImages


session = loadSession()

table_name = "sanfran_crimes_images_weeks_grayscale"

CrimesImages_ = create_image_table(table_name)
Base.metadata.create_all(engine)


def createHeatmaps(crime, table):
    """
    Create heatmaps with the data from the table.

    Args:
        crime - specific crime to crawl for
        table - table to get the data from
    """
    print("-"*5, "Started", crime, "-"*5)

    sql_ = """
    SELECT
    to_char(to_date(date, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY') as year,
    to_char(to_date(date, 'YYYY-MM-DD HH24:MI:SS'), 'MM') as month,
    to_char(to_date(date, 'YYYY-MM-DD HH24:MI:SS'), 'DD') as day,
    to_char(to_date(date, 'YYYY-MM-DD HH24:MI:SS'), 'WW') as woy,
    x,
    y
    FROM {}
    WHERE category = '{}'
    ORDER BY year, month, day;""".format(table, crime)
    df = pd.read_sql(sql_, engine)

    df = df[(df != 0.0).all(1)]
    df = df.dropna()

    data_x_max = df["x"].max()
    data_x_min = df["x"].min()

    data_y_max = df["y"].max()
    data_y_min = df["y"].min()

    granularity = 32
    image_array = np.zeros((granularity, granularity))

    old_woy = -1
    old_year = -1

    count = 0

    for index, row in df.iterrows():

        if old_woy == -1:
            old_woy = row["woy"]
        if old_year == -1:
            old_year = row["year"]
        if old_woy != row["woy"]:
            if DEBUG:
                print("Week of the year:", row["woy"], "/", row["year"],
                      "Amount of crimes:", count)

            image_array = np.ravel(image_array)

            new_image = CrimesImages_(image=image_array, woy=old_woy,
                                      year=old_year, crime=crime)
            session.add(new_image)

            image_array = np.zeros((granularity, granularity))
            old_year = row["year"]
            old_woy = row["woy"]
            count = 0

        pos_n = int((row["x"] - data_x_min) /
                    (data_x_max - data_x_min) * (granularity - 1))
        pos_m = int((row["y"] - data_y_min) /
                    (data_y_max - data_y_min) * (granularity - 1))

        image_array[pos_n, pos_m] = image_array[pos_n, pos_m] + 1
        count += 1

    print("-"*5, "Finished", crime, "-"*5)

    session.commit()


if __name__ == "__main__":

    table__ = "sanfran_crimes"
    crimes__ = get_table(table__)

    func_ = func.count(crimes__.category).label("count")
    query_ = session.query(crimes__.category, func_)
    result = query_.group_by(crimes__.category).order_by('count DESC')

    for x in result:
        if x[1] > 10000:
            createHeatmaps(x[0], table__)
