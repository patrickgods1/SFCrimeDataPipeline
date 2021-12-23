# Import various airflow modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Work with CSV files
import csv
# Work with date and time
from datetime import datetime, timedelta
# Work with OS functions
import os
# Work with HTTP requests
import requests
# Work with Dataframes
import pandas as pd
# Work with Google Sheets
import pygsheets

def fetchDataToLocal():
    """
    Use the Python requests library to download the data in CSV format and saved in the
    local data directory.
    """
    
    # Fetch the request
    url = "https://data.sfgov.org/api/views/wg3w-h783/rows.csv?accessType=DOWNLOAD&bom=false&format=false&delimiter=%7C"
    with requests.get(url, stream=True) as response:
        response.raise_for_status()

        with open(f"/opt/airflow/data/SFCrimeData2018toPresent.csv", "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
            file.flush()


def sqlLoad():
    # Connection to the PostgreSQL, defined in the Airflow UI
    conn = PostgresHook(postgres_conn_id="postgres_dwh").get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                    CREATE TABLE IF NOT EXISTS "Staging" (
                        "Incident Datetime" TIMESTAMP,
                        "Incident Date" DATE,
                        "Incident Time" TIME,
                        "Incident Year" INT,
                        "Incident Day of Week" VARCHAR(9),
                        "Report Datetime" TIMESTAMP,
                        "Row ID" BIGINT,
                        "Incident ID" INT,
                        "Incident Number" BIGINT,
                        "CAD Number" INT DEFAULT NULL,
                        "Report Type Code" CHAR(2),
                        "Report Type Description" VARCHAR(19),
                        "Filed Online" BOOL DEFAULT FALSE,
                        "Incident Code" INT,
                        "Incident Category" TEXT,
                        "Incident Subcategory" TEXT,
                        "Incident Description" TEXT,
                        "Resolution" TEXT,
                        "Intersection" TEXT,
                        "CNN" NUMERIC DEFAULT NULL,
                        "Police District" TEXT,
                        "Analysis Neighborhood" TEXT DEFAULT NULL,
                        "Supervisor District" INT DEFAULT NULL,
                        "Latitude" 	FLOAT8 DEFAULT NULL,
                        "Longitude" FLOAT8 DEFAULT NULL,
                        "Point" TEXT DEFAULT NULL,
                        "Neighborhoods" INT DEFAULT NULL,
                        "ESNCAG - Boundary File" SMALLINT DEFAULT NULL,
                        "Central Market/Tenderloin Boundary Polygon - Updated" SMALLINT DEFAULT NULL,
                        "Civic Center Harm Reduction Project Boundary" SMALLINT DEFAULT NULL,
                        "HSOC Zones as of 2018-06-05" SMALLINT DEFAULT NULL,
                        "Invest In Neighborhoods (IIN) Areas" SMALLINT DEFAULT NULL,
                        "Current Supervisor Districts" SMALLINT DEFAULT NULL,
                        "Current Police Districts" SMALLINT DEFAULT NULL);
                    
                    TRUNCATE TABLE "Staging";
                """)

            cur.execute("""
                        ALTER TABLE "Staging" 
                        DROP COLUMN IF EXISTS id;
                    """)

            with open(f"/opt/airflow/data/SFCrimeData2018toPresent.csv") as data:
                cur.copy_expert("""COPY "Staging"
                                FROM STDIN WITH (delimiter '|', format csv, header, NULL '')""", data)
            
            cur.execute("""
                        ALTER TABLE "Staging" ADD id SERIAL;
                    """)

    except:
        conn.rollback()
        raise

    else:
        conn.commit()

    finally:
        conn.close()


def sqlTransform():
    # Connection to the PostgreSQL, defined in the Airflow UI
    conn = PostgresHook(postgres_conn_id="postgres_dwh").get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                    CREATE TABLE IF NOT EXISTS "DimDate" (
                        "FullDate" DATE,
                        "DateID" INT,
                        "DayNameOfWeek" VARCHAR(9),
                        "DayNameOfWeekShort" CHAR(3),
                        "DayNumberOfMonth" SMALLINT,
                        "DayNumberOfWeek" SMALLINT,
                        "DayNumberOfYear" SMALLINT,
                        "HolidayName" TEXT DEFAULT NULL,                        
                        "isHoliday" BOOL,
                        "isWeekday" BOOL,
                        "isWeekend" BOOL,
                        "MonthNameShort" CHAR(3),
                        "isEndOfMonth" BOOL,
                        "MonthName" VARCHAR(9),
                        "MonthNumberOfYear" SMALLINT,
                        "CalendarQuarterNumber" SMALLINT,
                        "CalendarQuarterName" VARCHAR(6),
                        "CalendarQuarterShortName" CHAR(2),
                        "SameDayPreviousYear" DATE,
                        "Season" VARCHAR(6),
                        "WeekBeginDate" DATE,
                        "WeekNumberOfMonth" SMALLINT,
                        "WeekNumberOfYear" SMALLINT,
                        "CalenderYear" SMALLINT);

                    TRUNCATE TABLE "DimDate";
                """)

            with open(f"/opt/airflow/data/dimDate.csv") as data:
                cur.copy_expert("""COPY "DimDate"
                                FROM STDIN WITH (delimiter ',', format csv, header, NULL '')""", data)

            cur.execute("""
                    CREATE TABLE IF NOT EXISTS "DimTime" (
                        "TimeID" SERIAL,
                        "Hour24" SMALLINT,
                        "Hour12" SMALLINT,
                        "Minute" SMALLINT,
                        "Second" SMALLINT,
                        "AMPM" CHAR(2),
                        "FullTime24" TIME,
                        "FullTime12" VARCHAR(11),
                        "TimeOfDay" VARCHAR(9));
                    
                    TRUNCATE TABLE "DimTime";
                """)

            with open(f"/opt/airflow/data/dimTime.csv") as data:
                cur.copy_expert("""COPY "DimTime"
                                FROM STDIN WITH (delimiter ',', format csv, header, NULL '')""", data)

            cur.execute("""
                    CREATE TABLE IF NOT EXISTS "DimLocation"
                    ("LocationID" INT,
                    "PoliceDistrict" TEXT,
                    "AnalysisNeighborhood" TEXT);

                    TRUNCATE TABLE "DimLocation";

                    INSERT INTO "DimLocation"
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY "Police District", "Analysis Neighborhood") AS "LocationID",
                        "Police District" AS "PoliceDistrict",
                        "Analysis Neighborhood" AS "AnalysisNeighborhood"
                    FROM
                        (SELECT DISTINCT "Police District", "Analysis Neighborhood"
                            FROM "Staging") t;
                """)

            cur.execute("""
                    CREATE TABLE IF NOT EXISTS "DimIncident"
                        ("IncidentID" INT,
                        "IncidentCategory" TEXT,
                        "IncidentSubcategory" TEXT,
                        "Resolution" TEXT);
                    
                    TRUNCATE TABLE "DimIncident";

                    INSERT INTO "DimIncident"
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY "Incident Category", "Incident Subcategory", "Resolution") AS "IncidentID",
                        "Incident Category" AS "IncidentCategory",
                        "Incident Subcategory" AS "IncidentSubcategory",
                        "Resolution"
                    FROM (SELECT DISTINCT "Incident Category", "Incident Subcategory", "Resolution"
                            FROM "Staging") t;
                """)

            cur.execute("""
                    CREATE TABLE IF NOT EXISTS "DimReportType"
                        ("ReportTypeID" INT, 
                        "ReportType" VARCHAR(19), 
                        "ReportTypeCode" CHAR(2), 
                        "FiledOnline" BOOL);

                    TRUNCATE TABLE "DimReportType";

                    INSERT INTO "DimReportType"
                    SELECT
                        ROW_NUMBER() OVER (ORDER BY "Report Type Description", "Report Type Code", "Filed Online") AS "ReportTypeID",
                        "Report Type Description" AS "ReportType",
                        "Report Type Code" AS "ReportTypeCode",
                        CASE
                            WHEN "Filed Online" IS NULL THEN false
                            ELSE "Filed Online"
                        END AS "FiledOnline"
                    FROM (SELECT DISTINCT "Report Type Description", "Report Type Code", "Filed Online"
                            FROM "Staging") t;
                """)

            cur.execute("""
                    CREATE TABLE IF NOT EXISTS "FactCrime"
                        ("CrimeID" INT,
                        "IncidentDateID" INT,
                        "IncidentTimeID" INT,
                        "ReportDateID" INT,
                        "ReportTimeID" INT,
                        "LocationID" INT,
                        "IncidentID" INT,
                        "ReportTypeID" INT,
                        "IncidentDescription" TEXT,
                        "Intersection" TEXT, 
                        "Latitude" FLOAT8, 
                        "Longitude" FLOAT8);

                    TRUNCATE TABLE "FactCrime";

                    INSERT INTO "FactCrime"
                    SELECT
                        s.id,
                        d1."DateID" AS "IncidentDateID",
                        t1."TimeID" AS "IncidentTimeID",
                        d2."DateID" AS "ReportDateID", 
                        t1."TimeID" AS "ReportTimeID",
                        l."LocationID" AS "LocationID",
                        i."IncidentID" AS "IncidentID",
                        r."ReportTypeID" AS "ReportTypeID",
                        s."Incident Description" AS "IncidentDescription",
                        s."Intersection",
                        s."Latitude",
                        s."Longitude"
                    FROM "Staging" s
                        LEFT JOIN "DimDate" AS d1 ON s."Incident Date" = d1."FullDate"
                        LEFT JOIN "DimTime" AS t1 ON (s."Incident Time" = t1."FullTime24")
                        LEFT JOIN "DimDate" AS d2 ON s."Report Datetime"::DATE = d2."FullDate"
                        LEFT JOIN "DimTime" AS t2 ON (s."Report Datetime"::TIME = t2."FullTime24")
                        LEFT JOIN "DimIncident" AS i ON s."Incident Category" = i."IncidentCategory"
                            AND s."Incident Subcategory" = i."IncidentSubcategory"
                            AND s."Resolution" = i."Resolution"
                        LEFT JOIN "DimLocation" AS l ON s."Police District" = l."PoliceDistrict"
                            AND s."Analysis Neighborhood" = l."AnalysisNeighborhood"
                        LEFT JOIN "DimReportType" AS r ON s."Report Type Description" = r."ReportType"
                            AND s."Report Type Code" = r."ReportTypeCode";
                """)

    except:
        conn.rollback()
        raise

    else:
        conn.commit()

    finally:
        conn.close()


def fetchToGSheets():
    # Connection to the PostgreSQL, defined in the Airflow UI
    conn = PostgresHook(postgres_conn_id="postgres_dwh").get_conn()

    try:
        # sql = """SELECT f."IncidentDescription",
        #                 f."Intersection",
        #                 f."Latitude",
        #                 f."Longitude",
        #                 idate."FullDate" AS "IncidentFullDate",
        #                 idate."DayNameOfWeek" AS "IncidentDayNameOfWeek",
        #                 idate."DayNameOfWeekShort" AS "IncidentDayNameOfWeekShort",
        #                 idate."DayNumberOfMonth" AS "IncidentDayNumberOfMonth",
        #                 idate."DayNumberOfWeek" AS "IncidentDayNumberOfWeek",
        #                 idate."DayNumberOfYear" AS "IncidentDayNumberOfYear",
        #                 idate."HolidayName" AS "IncidentHolidayName",
        #                 idate."isHoliday" AS "IncidentisHoliday",
        #                 idate."isWeekday" AS "IncidentisWeekday",
        #                 idate."isWeekend" AS "IncidentisWeekend",
        #                 idate."MonthNameShort" AS "IncidentMonthNameShort",
        #                 idate."isEndOfMonth" AS "IncidentisEndOfMonth",
        #                 idate."MonthName" AS "IncidentMonthName",
        #                 idate."MonthNumberOfYear" AS "IncidentMonthNumberOfYear",
        #                 idate."CalendarQuarterNumber" AS "IncidentCalendarQuarterNumber",
        #                 idate."CalendarQuarterName" AS "IncidentCalendarQuarterName",
        #                 idate."CalendarQuarterShortName" AS "IncidentCalendarQuarterShortName",
        #                 idate."SameDayPreviousYear" AS "IncidentSameDayPreviousYear",
        #                 idate."Season" AS "IncidentSeason",
        #                 idate."WeekBeginDate" AS "IncidentWeekBeginDate",
        #                 idate."WeekNumberOfMonth" AS "IncidentWeekNumberOfMonth",
        #                 idate."WeekNumberOfYear" AS "IncidentWeekNumberOfYear",
        #                 idate."CalenderYear" AS "IncidentCalenderYear",
        #                 itime."TimeID" AS "IncidentTimeID",
        #                 itime."Hour24" AS "IncidentHour24",
        #                 itime."Hour12" AS "IncidentHour12",
        #                 itime."Minute" AS "IncidentMinute",
        #                 itime."Second" AS "IncidentSecond",
        #                 itime."AMPM" AS "IncidentAMPM",
        #                 itime."FullTime24" AS "IncidentFullTime24",
        #                 itime."FullTime12" AS "IncidentFullTime12",
        #                 itime."TimeOfDay" AS "IncidentTimeOfDay",
        #                 rdate."FullDate" AS "ReportFullDate",
        #                 rdate."DayNameOfWeek" AS "ReportDayNameOfWeek",
        #                 rdate."DayNameOfWeekShort" AS "ReportDayNameOfWeekShort",
        #                 rdate."DayNumberOfMonth" AS "ReportDayNumberOfMonth",
        #                 rdate."DayNumberOfWeek" AS "ReportDayNumberOfWeek",
        #                 rdate."DayNumberOfYear" AS "ReportDayNumberOfYear",
        #                 rdate."HolidayName" AS "ReportHolidayName",
        #                 rdate."isHoliday" AS "ReportisHoliday",
        #                 rdate."isHoliday" AS "ReportisHoliday",
        #                 rdate."isWeekend" AS "ReportisWeekend",
        #                 rdate."MonthNameShort" AS "ReportMonthNameShort",
        #                 rdate."isEndOfMonth" AS "ReportisEndOfMonth",
        #                 rdate."MonthName" AS "ReportMonthName",
        #                 rdate."MonthNumberOfYear" AS "ReportMonthNumberOfYear",
        #                 rdate."CalendarQuarterNumber" AS "ReportCalendarQuarterNumber",
        #                 rdate."CalendarQuarterName" AS "ReportCalendarQuarterName",
        #                 rdate."CalendarQuarterShortName" AS "ReportCalendarQuarterShortName",
        #                 rdate."SameDayPreviousYear" AS "ReportSameDayPreviousYear",
        #                 rdate."Season" AS "ReportSeason",
        #                 rdate."WeekBeginDate" AS "ReportWeekBeginDate",
        #                 rdate."WeekNumberOfMonth" AS "ReportWeekNumberOfMonth",
        #                 rdate."WeekNumberOfYear" AS "ReportWeekNumberOfYear",
        #                 rdate."CalenderYear" AS "ReportCalenderYear",
        #                 rtime."TimeID" AS "ReportTimeID",
        #                 rtime."Hour24" AS "ReportHour24",
        #                 rtime."Hour12" AS "ReportHour12",
        #                 rtime."Minute" AS "ReportMinute",
        #                 rtime."Second" AS "ReportSecond",
        #                 rtime."AMPM" AS "ReportAMPM",
        #                 rtime."FullTime24" AS "ReportFullTime24",
        #                 rtime."FullTime12" AS "ReportFullTime12",
        #                 rtime."TimeOfDay" AS "ReportTimeOfDay",
        #                 l."PoliceDistrict",
        #                 l."AnalysisNeighborhood",
        #                 i."IncidentCategory",
        #                 i."IncidentSubcategory",
        #                 i."Resolution",
        #                 r."ReportType",
        #                 r."ReportTypeCode",
        #                 r."FiledOnline"
        #         FROM "FactCrime" f
        #         JOIN "DimDate" idate ON  f."IncidentDateID" = idate."DateID"
        #         JOIN "DimTime" itime ON f."IncidentTimeID" = itime."TimeID"
        #         JOIN "DimDate" rdate ON  f."ReportDateID" = rdate."DateID"
        #         JOIN "DimTime" rtime ON f."ReportTimeID" = rtime."TimeID"
        #         JOIN "DimLocation" l ON f."LocationID" = l."LocationID"
        #         JOIN "DimIncident" i ON f."IncidentID" = i."IncidentID"
        #         JOIN "DimReportType" r ON f."ReportTypeID" = r."ReportTypeID";
        #     """
        sql = """SELECT f."IncidentDescription",
                        f."Intersection",
                        f."Latitude",
                        f."Longitude",
                        idate."FullDate" AS "IncidentFullDate",
                        idate."DayNameOfWeek" AS "IncidentDayNameOfWeek",
                        idate."DayNumberOfMonth" AS "IncidentDayNumberOfMonth",
                        idate."HolidayName" AS "IncidentHolidayName",
                        idate."isWeekend" AS "IncidentisWeekend",
                        idate."MonthName" AS "IncidentMonthName",
                        idate."CalenderYear" AS "IncidentCalenderYear",
                        itime."Hour24" AS "IncidentHour24",
                        itime."FullTime12" AS "IncidentFullTime12",
                        itime."TimeOfDay" AS "IncidentTimeOfDay",
                        l."PoliceDistrict",
                        l."AnalysisNeighborhood",
                        i."IncidentCategory",
                        i."IncidentSubcategory",
                        r."ReportType"
                FROM "FactCrime" f
                JOIN "DimDate" idate ON  f."IncidentDateID" = idate."DateID"
                JOIN "DimTime" itime ON f."IncidentTimeID" = itime."TimeID"
                JOIN "DimLocation" l ON f."LocationID" = l."LocationID"
                JOIN "DimIncident" i ON f."IncidentID" = i."IncidentID"
                JOIN "DimReportType" r ON f."ReportTypeID" = r."ReportTypeID"
                WHERE r."ReportType" IN ('Coplogic Initial', 'Initial', 'Vehicle Initial');
            """
        df = pd.read_sql(sql, conn)
        dirpath = os.getcwd()
        gc = pygsheets.authorize(service_file=f'{dirpath}/config/service_file.json')
        sh = gc.open('SFCrimeData')
        wks = sh[0]
        wks.clear(start='A1', end=None, fields='*')
        # wks.rows = df.shape[0]
        wks.set_dataframe(df,(0,0), fit=True)
    except:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

default_args = {
    "owner": "airflow",
    "start_date": datetime.today() - timedelta(days=1)
              }
with DAG(
    "SFCrimeDataELT",
    default_args = default_args,
    schedule_interval = "0 19 * * *",
    ) as dag:
    fetchDataToLocal = PythonOperator(
            task_id = "fetch_data_to_local",
            python_callable = fetchDataToLocal
        )
    sqlLoad = PythonOperator(
            task_id = "sql_load",
            python_callable = sqlLoad
        )
    sqlTransform = PythonOperator(
            task_id = "sql_transform",
            python_callable = sqlTransform
        )
    fetchToGSheets = PythonOperator(
            task_id = "fetch_to_gsheets",
            python_callable = fetchToGSheets
        )

fetchDataToLocal >> sqlLoad >> sqlTransform >> fetchToGSheets