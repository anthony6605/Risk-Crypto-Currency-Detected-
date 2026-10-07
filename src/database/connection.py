import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


load_dotenv()


def get_engine():

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRESQL_USER"),
        password=os.getenv("POSTGRESQL_PASSWORD"),
        host=os.getenv("POSTGRESQL_HOST"),
        port=int(
            os.getenv(
                "POSTGRESQL_PORT",
                "5433",
            )
        ),
        database=os.getenv("POSTGRESQL_DB"),
    )

    engine = create_engine(
        database_url,
        pool_pre_ping=True,
    )

    return engine