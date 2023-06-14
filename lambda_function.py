import boto3
import pandas as pd
import awswrangler as wr
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_event_date(event):
    """Parses a date from the event data.

    Args:
        event (dict): The event data.

    Returns:
        datetime: The parsed date, or the current date
        plus one month if no date is provided.
    """
    if "data_ref" in event and event["data_ref"]:
        return datetime.strptime(event["data_ref"], "%Y%m")

    current_date = datetime.now()
    next_month = current_date + relativedelta(months=1)
    return next_month


def generate_month_data(year, month):
    """Generates data for the entire month.

    Args:
        year (int): The year.
        month (int): The month.

    Returns:
        list: The data for the month.
    """
    _, days_in_month = monthrange(year, month)
    data = []

    for day in range(1, days_in_month + 1):
        for hour in range(24):
            trimester = (month - 1) // 3 + 1
            semester = (month - 1) // 6 + 1

            data.append(
                {
                    "cod_idef_temp": len(data) + 1,
                    "dat_temp": datetime(year, month, day, hour),
                    "hor_temp": hour,
                    "num_dia_temp": day,
                    "num_mes_temp": month,
                    "num_ano_temp": year,
                    "num_trme_temp": trimester,
                    "num_sems_temp": semester,
                    "ano": year,
                    "mes": month,
                    "dia": day,
                }
            )

    return data


def save_to_s3(df, database_name, table_name, s3_location, boto3_session):
    """Save a dataframe to an S3 location.

    Args:
        df (DataFrame): The dataframe to save.
        database_name (str): The name of the database.
        table_name (str): The name of the table.
        s3_location (str): The S3 location to save to.
        boto3_session (Session): The boto3 session.
    """
    try:
        logger.info(df.info())
        wr.s3.to_parquet(
            df=df,
            path=s3_location,
            table=table_name,
            database=database_name,
            compression="snappy",
            dataset=True,
            schema_evolution=False,
            partition_cols=["ano", "mes", "dia"],
            boto3_session=boto3_session,
            mode="overwrite_partitions",
        )
    except Exception as e:
        logger.error(f"Failed to save data: {e}")
        raise ValueError(f"Failed to save data: {e}")


def lambda_handler(event, context):
    """The Lambda function handler.

    Args:
        event (dict): The event data.
        context (object): The Lambda context.
    """
    # boto 3 session
    boto3_session = boto3.Session()

    dt = parse_event_date(event)
    year = dt.year
    month = dt.month

    data = generate_month_data(year, month)
    df = pd.DataFrame(data)

    database_name = "db_dimensao"
    table_name = "dim_tempo"

    client = boto3.client("glue")
    response = client.get_table(DatabaseName=database_name, Name=table_name)
    s3_location = response["Table"]["StorageDescriptor"]["Location"]

    save_to_s3(df, database_name, table_name, s3_location, boto3_session)
