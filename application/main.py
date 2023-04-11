import logging
import csv
from datetime import date

from sqlalchemy import text, desc
from sqlalchemy.orm import Session

from application.database import Base, engine, db_session
from application.models.competition import Competition
from application.models.date import Date
from application.models.open import Open
from application.models.promotion import Promotion
from application.models.promotion2 import Promotion2
from application.models.school_holiday import SchoolHoliday

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

    logging.info("Starting extract stage ...")

    # Extract store.csv
    with open("store.csv", newline="") as f:
        logging.info("Processing store.csv")
        store_reader = csv.reader(f)
        next(store_reader)
        for index, row in enumerate(store_reader):
            logging.debug("Processing row %s of store.csv", index)
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
            db_session.add(store)
        db_session.commit()

    # Extract train_large.csv
    with open("train.csv") as f:
        logging.info("Processing train.csv")
        train_reader = csv.reader(f)
        next(train_reader)
        for index, row in enumerate(train_reader):
            logging.debug("Processing row %s of train.csv", index)
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
            db_session.add(train)
        db_session.commit()

        # Source and Stage store
        logging.info("Source and stage store")
        StagingStore.__table__.drop(engine)
        db_session.execute(text("SELECT * INTO sta_store FROM sta_store_cur EXCEPT SELECT * FROM sta_store_prev;"))
        db_session.commit()

        # Source and Stage train
        logging.info("Source and stage train")
        StagingTrain.__table__.drop(engine)
        db_session.execute(text("SELECT * INTO sta_train FROM sta_train_cur EXCEPT SELECT * FROM sta_train_prev;"))
        db_session.commit()

    logging.info("Extract stage done.")


def transform():
    """Transform stage."""

    logging.info("Starting transform stage...")

    logging.info("Transform store data...")
    rows = db_session.query(StagingStore).all()
    count = len(rows)
    for index, row in enumerate(rows):
        logging.debug("Transforming %s from %s", index, count)

        # Transform Promo2 to boolean
        row.Promo2 = bool(row.Promo2)

        # Transform Promo2SinceYear and Promo2SinceWeek to Promo2SinceWeekYear
        if row.Promo2SinceYear and row.Promo2SinceWeek:
            row.Promo2SinceWeekYear = f"{row.Promo2SinceYear}W{row.Promo2SinceWeek}"

        # Transform CompetitionDistance to postive integer
        if row.CompetitionDistance and int(row.CompetitionDistance) > 0:
            row.CompetitionDistance = int(row.CompetitionDistance)
        else:
            row.CompetitionDistance = None

        # Transform CompetitionOpenSinceMonth and CompetitionOpenSinceYear to CompetitionOpenSinceMonthYear
        if row.CompetitionOpenSinceMonth and row.CompetitionOpenSinceYear:
            row.CompetitionOpenSinceMonthYear = f"{row.CompetitionOpenSinceYear}-{row.CompetitionOpenSinceMonth}"

        # Transform Store to
        row.Store = int(row.Store)

        # Transform StoreType to stripped string
        row.StoreType = str(row.StoreType).strip()

        # Transform Assortment to stripped string
        row.Assortment = str(row.Assortment).strip()

        db_session.merge(row)
    db_session.commit()

    logging.info("Transform train data...")
    rows = db_session.query(StagingTrain).all()
    count = len(rows)
    for index, row in enumerate(rows):
        logging.debug("Transforming %s from %s", index, count)

        # Transform Date to Day, Year, Week, Quarter, DayOfWeek
        if row.Date:
            row.Day = row.Date.day
            row.Month = row.Date.month
            row.Year = row.Date.year
            row.Week = row.Date.isocalendar()[1]
            # Get the quarter of the year
            row.Quarter = (row.Date.month - 1) // 3 + 1
            # Get the day of the week (0 is Monday, 6 is Sunday)
            row.DayOfWeek = row.Date.weekday()

        # Transform SchoolHoliday to boolean
        row.SchoolHoliday = bool(row.SchoolHoliday)

        # Transform Promo to boolean
        row.Promo = bool(row.Promo)

        # Transform Open to boolean
        row.Open = bool(row.Open)

        db_session.merge(row)
    db_session.commit()

    logging.info("Transform stage done.")


def load():
    """Load stage."""

    logging.info("Starting load stage...")

    with Session(engine) as session:
        logging.info("Load staged store data...")
        rows = session.query(StagingStore).all()
        count = len(rows)
        for index, row in enumerate(rows):
            logging.debug("Loading %s from %s", index, count)

            # Load Promotion2 (SCD Type 2)
            is_promotion = row.Promo2
            since_week_year = row.Promo2SinceWeekYear
            interval = row.PromoInterval
            promotion2 = session.query(Promotion2).filter(
                    Promotion2.IsPromotion == is_promotion,
                    Promotion2.SinceWeekYear == since_week_year,
                    Promotion2.Interval == interval
            ).first()
            if not promotion2:
                # If there is no matching Promotion2, create a new entry
                promotion2 = Promotion2(
                    IsPromotion=is_promotion,
                    SinceWeekYear=since_week_year,
                    Interval=interval,
                )
                session.add(promotion2)
                session.commit()

            # Load Competition (SCD Type 2)
            competition_distance = row.CompetitionDistance

            open_since_month_year = row.CompetitionOpenSinceMonthYear
            competition = session.query(Competition).filter(
                Competition.CompetitionDistance == competition_distance,
                Competition.OpenSinceMonthYear == open_since_month_year
            ).first()
            if not competition:
                # If there is no matching Competition, create a new entry
                competition = Competition(
                    CompetitionDistance=competition_distance,
                    OpenSinceMonthYear=open_since_month_year
                )
                session.add(competition)
                session.commit()

            # Load Store (SCD Type 2)
            store_nr = row.Store
            store_type = row.StoreType
            assortment = row.Assortment
            store = session.query(Store).filter(
                Store.StoreNr == store_nr,
                Store.StoreType == store_type,
                Store.Assortment == assortment
            ).first()
            if not store:
                # If there is no matching Store, create a new entry
                store = Store(
                    Promotion2ID=promotion2.Promotion2ID,
                    CompetitionID=competition.CompetitionID,
                    StoreType=store_type,
                    Assortment=assortment,
                    StoreNr=store_nr
                )
                session.add(store)
                session.commit()

        logging.info("Load staged train data...")
        rows = session.query(StagingTrain).all()
        count = len(rows)
        for index, row in enumerate(rows):
            logging.debug("Loading %s from %s", index, count)

            # Load Date (No SCD)
            day = row.Day
            month = row.Month
            year = row.Year
            week = row.Week
            quarter = row.Quarter
            day_of_week = row.DayOfWeek
            _date = session.query(Date).filter(
                Date.Day == day,
                Date.Month == month,
                Date.Year == year,
                Date.Week == week,
                Date.Quarter == quarter,
                Date.DayOfWeek == day_of_week
            ).first()
            if not _date:
                _date = Date(
                    Day=day,
                    Month=month,
                    Year=year,
                    Week=week,
                    Quarter=quarter,
                    DayOfWeek=day_of_week
                )
                session.add(_date)
                session.commit()

            # Load SchoolHoliday (No SCD)
            is_school_holiday = row.SchoolHoliday
            school_holiday = session.query(SchoolHoliday).filter(
                SchoolHoliday.IsSchoolHoliday == is_school_holiday
            ).first()
            if not school_holiday:
                school_holiday = SchoolHoliday(
                    IsSchoolHoliday=is_school_holiday
                )
                session.add(school_holiday)
                session.commit()

            # Load Promotion (No SCD)
            is_promotion = row.Promo
            promotion = session.query(Promotion).filter(
                Promotion.IsPromotion == is_promotion
            ).first()
            if not promotion:
                promotion = Promotion(
                    IsPromotion=is_promotion
                )
                session.add(promotion)
                session.commit()

            # Load Open (No SCD)
            is_open = row.Open
            _open = session.query(Open).filter(
                Open.IsOpen == is_open
            ).first()
            if not _open:
                _open = Open(
                    IsOpen=is_open
                )
                session.add(_open)
                session.commit()

            # Load StoreSales Fact (No SCD)
            store = session.query(Store).filter(
                Store.StoreNr == row.Store
            ).order_by(desc(Store.StoreID)).first()
            if not store:
                continue
            store_sales = StoreSales(
                StoreID=store.StoreID,
                DateID=_date.DateID,
                OpenID=_open.OpenID,
                PromotionID=promotion.PromotionID,
                SchoolHolidayID=school_holiday.SchoolHolidayID,
                Sales=row.Sales,
                Customers=row.Customers
            )
            session.add(store_sales)
            session.commit()

    logging.info("Load stage done.")


def post_process():
    """Post-processing stage."""
    logging.info("Starting post-processing...")

    with engine.connect() as connection:
        dialect = connection.dialect.name

    with Session(engine) as session:
        # Drop sta_store_prev
        StagingStorePrev.__table__.drop(engine)

        if dialect == "mssql":
            # Copy sta_store_cur to sta_store_prev
            session.execute(text(f"EXEC sp_rename 'sta_store_cur', 'sta_store_prev';"))

        # Drop sta_train_prev
        StagingTrainPrev.__table__.drop(engine)

        if dialect == "mssql":
            # Copy sta_train_cur to sta_train_prev
            session.execute(text(f"EXEC sp_rename 'sta_train_cur', 'sta_train_prev';"))

        session.commit()

    logging.info("Post-processing done.")


if __name__ == "__main__":
    main()
