import pandas as pd

from hamilton.function_modifiers import extract_columns


DRIVER_SOURCE_COLUMNS = [
    "event_timestamp",
    "driver_id",
    "conv_rate",
    "acc_rate",
    "avg_daily_trips",
    "created",
]


@extract_columns(*DRIVER_SOURCE_COLUMNS)
def driver_dataset(driver_dataset_path: str) -> pd.DataFrame:
    """Load the driver dataset"""
    return pd.read_parquet(driver_dataset_path)


def trip_day_of_week(event_timestamp: pd.Series) -> pd.Series:
    """Encode day of the week of the trip as int"""
    return pd.Series(event_timestamp.dt.day_of_week, name="trip_day_of_week")


def trip_month(event_timestamp: pd.Series) -> pd.Series:
    """Encode month of the trip as int"""
    return event_timestamp.dt.month


def trip_quarter(trip_month: pd.Series) -> pd.Series:
    """Encode quarter of the trip as int"""
    encoding = {
        0: 0, 1: 0, 3: 0,
        4: 1, 5: 1, 6: 1,
        7: 2, 8: 2, 9: 2,
        10: 3, 11: 3, 12: 3,
    }
    return trip_month.replace(encoding)


def max_conv_rate_per_day_of_week(
    driver_id: pd.Series,
    conv_rate: pd.Series,
    trip_day_of_week: pd.Series,
) -> pd.Series:
    """Compute the max conversion rate per (user, day of the week) and broadcast values"""
    df = pd.concat([driver_id, conv_rate, trip_day_of_week], axis=1)
    return df.groupby(["driver_id", "trip_day_of_week"])["conv_rate"].transform("max")


def percentile_conv_rate(conv_rate: pd.Series):
    return conv_rate.rank(pct=True)


def max_acc_rate_per_day_of_week(
    driver_id: pd.Series,
    acc_rate: pd.Series,
    trip_day_of_week: pd.Series,
) -> pd.Series:
    """Compute the max acc rate per (user, day of the week) and broadcast values"""
    df = pd.concat([driver_id, acc_rate, trip_day_of_week], axis=1)
    return df.groupby(["driver_id", "trip_day_of_week"])["acc_rate"].transform("max")



# the output dataframe could be a Feast FeatureView (FV) since the DAG implies:
# -- FV name = function name
# -- FV description = function docstring
# -- FV entity = driver_id
# -- FV schema = [trip_day..., max_conv..., max_acc...]
# -- FV source = driver_dataset
# FV don't need a specified timestamp since its associated with the FV source.
# However, the source's timestamp needs to be part of the table
# There will be missing information about the time-to-live (TTL), if online/offline
# and additional metadata like tags
def feast_stats_per_day_of_week(
    event_timestamp: pd.Series,
    driver_id: pd.Series,
    trip_day_of_week: pd.Series,
    max_conv_rate_per_day_of_week: pd.Series,
    max_acc_rate_per_day_of_week: pd.Series,
) -> pd.DataFrame:
    """Driver statistics by day of the week"""
    df = pd.concat([
        event_timestamp,
        driver_id,
        trip_day_of_week,
        max_conv_rate_per_day_of_week,
        max_acc_rate_per_day_of_week,
    ], axis=1)
    return df
 