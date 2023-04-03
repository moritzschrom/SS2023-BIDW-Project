import logging
import csv
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from application.database import Base, engine

from application.models.store import Store
from application.models.store_sales import StoreSales
from application.models.staging_store import StagingStore, StagingStorePrev, StagingStoreCur
from application.models.staging_train import StagingTrain, StagingTrainPrev, StagingTrainCur

logging.basicConfig(level=logging.DEBUG)


def main():
    """Main entry point for the ETL process."""

    logging.info("Setup database...")
    Base.metadata.create_all(engine)

    logging.info("Starting ETL process for Rossmann Business Intelligence Data Warehouse...")

    pre_process()

    extract()
    transform()
    load()

    post_process()


def pre_process():
    """Pre-processing stage."""

    logging.info("Starting pre-processing stage...")

    with Session(engine) as session:
        # Truncate sta_store
        session.query(StagingStore).delete()

        # Truncate sta_store_cur
        session.query(StagingStoreCur).delete()

        # Truncate sta_train
        session.query(StagingTrain).delete()

        # Truncate sta_train_cur
        session.query(StagingTrainCur).delete()

        # Commit changes
        session.commit()

    logging.info("Pre-processing stage done.")


def extract():
    """Extract stage."""

    logging.info("Starting extract stage...")

    with Session(engine) as session:
        # Extract store.csv
        with open("store.csv", newline="") as f:
            store_reader = csv.reader(f)
            next(store_reader)
            for row in store_reader:
                store = StagingStoreCur(
                    Store=int(row[0]),
                    StoreType=row[1],
                    Assortment=row[2],
                    CompetitionDistance=int(row[3]) if row[3] else None,
                    CompetitionOpenSinceMonth=int(row[4]) if row[4] else None,
                    CompetitionOpenSinceYear=int(row[5]) if row[5] else None,
                    Promo2=bool(row[6]),
                    Promo2SinceWeek=int(row[7]) if row[7] else None,
                    Promo2SinceYear=int(row[8]) if row[8] else None,
                    PromoInterval=row[9] if row[9] else None
                )
                session.add(store)
            session.commit()

        # Extract train.csv
        with open("train.csv") as f:
            train_reader = csv.reader(f)
            next(train_reader)
            for row in train_reader:
                train = StagingTrainCur(
                    Store=int(row[0]),
                    DayOfWeek=int(row[1]),
                    Date=date.fromisoformat(row[2]),
                    Sales=int(row[3]),
                    Customers=int(row[4]),
                    Open=bool(row[5]),
                    Promo=bool(row[6]),
                    StateHoliday=bool(row[7]),
                    SchoolHoliday=bool(row[8]),
                )
                session.add(train)
            session.commit()

    logging.info("Extract stage done.")


def transform():
    """Transform stage."""

    logging.info("Starting transform stage...")

    logging.info("Transform stage done.")


def load():
    """Load stage."""

    logging.info("Starting load stage...")

    logging.info("Load stage done.")


def post_process():
    """Post-processing stage."""
    logging.info("Starting post-processing...")

    with engine.connect() as connection:
        # Drop sta_store_prev
        StagingStorePrev.__table__.drop(connection)

        # Copy sta_store_cur to sta_store_prev
        connection.execute(text(f"EXECUTE sp_rename 'sta_store_cur', 'sta_store_prev';"))

        # Drop sta_train_prev
        StagingTrainPrev.__table__.drop(connection)

        # Copy sta_train_cur to sta_train_prev
        connection.execute(text(f"EXECUTE sp_rename 'sta_train_cur', 'sta_train_prev';"))

    logging.info("Post-processing done.")


if __name__ == "__main__":
    main()
