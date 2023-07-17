from datetime import timedelta

import pandas as pd
from feast import Entity, FeatureService, FeatureView, Field, FileSource, PushSource, RequestSource
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.types import Float32, Float64, Int64


def driver_entity() -> Entity:
    """Feast definition: driver entity"""
    return Entity(name="driver", join_keys=["driver_id"])


def driver_hourly_stats_source(driver_source_path: str) -> FileSource:
    """Feast definition: source with hourly stats of driver"""
    return FileSource(
        name="driver_hourly_stats",
        path=driver_source_path,
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )


def input_request_source() -> RequestSource:
    """Feast definition: mock feature values only available at request time"""
    return RequestSource(
        name="vals_to_add",
        schema=[
            Field(name="val_to_add", dtype=Int64),
            Field(name="val_to_add_2", dtype=Int64),
        ],
    )



def driver_stats_push_source(driver_hourly_stats_source: FileSource) -> PushSource:
    """Feast definition: push data to your store (offline, online, both)"""
    return PushSource(
        name="driver_stats_push",
        batch_source=driver_hourly_stats_source
    )



def driver_hourly_stats_fv(
    driver_entity: Entity,
    driver_hourly_stats_source: FileSource
) -> FeatureView:
    """Feast definition: feature view with hourly stats of driver"""
    return FeatureView(
        name="driver_hourly_stats",
        entities=[driver_entity],
        ttl=timedelta(days=1),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64, description="Average daily trips"),
        ],
        online=True,
        source=driver_hourly_stats_source,
        tags={"team": "driver_performance"},
    )


def driver_hourly_stats_fresh_fv(
    driver_entity: Entity,
    driver_stats_push_source: PushSource,
) -> FeatureView:
    """Feast definition: feature view with fresh hourly stats of driver from push source"""
    return FeatureView(
        name="driver_hourly_stats_fresh",
        entities=[driver_entity],
        ttl=timedelta(days=1),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
        ],
        online=True,
        source=driver_stats_push_source,  # Changed from above
        tags={"team": "driver_performance"},
    )


def _transformed_conv_rate_udf(df: pd.DataFrame) -> pd.DataFrame:
    """UDF to compute the adjusted conversion rate at request time"""
    out_df = pd.DataFrame()
    out_df["conv_rate_plus_val1"] = df["conv_rate"] + df["val_to_add"]
    out_df["conv_rate_plus_val2"] = df["conv_rate"] + df["val_to_add_2"]
    return out_df


def transformed_conv_rate(
    driver_hourly_stats_fv: FeatureView,
    input_request_source: RequestSource
) -> OnDemandFeatureView:
    """Feast definition: feature view with features only available at request time"""
    return OnDemandFeatureView(
        name="transformed_conv_rate",
        schema=[
            Field(name="conv_rate_plus_val1", dtype=Float64),
            Field(name="conv_rate_plus_val2", dtype=Float64),
        ],
        sources=[
            driver_hourly_stats_fv,
            input_request_source,
        ],
        udf=_transformed_conv_rate_udf
    )


def transformed_conv_rate_fresh(
    driver_hourly_stats_fresh_fv: FeatureView,
    input_request_source: RequestSource
) -> OnDemandFeatureView:
    """Feast definition: feature view with fresh data and
     features only available at request time"""
    return OnDemandFeatureView(
        name="transformed_conv_rate_fresh",
        schema=[
            Field(name="conv_rate_plus_val1", dtype=Float64),
            Field(name="conv_rate_plus_val2", dtype=Float64),
        ],
        sources=[
            driver_hourly_stats_fresh_fv,
            input_request_source,
        ],
        udf=_transformed_conv_rate_udf
    )


def driver_activity_v1_fs(
    driver_hourly_stats_fv: FeatureView,
    transformed_conv_rate: OnDemandFeatureView,
) -> FeatureService:
    """Feast definition: grouping of features relative to driver activity"""
    return FeatureService(
        name="driver_activity_v1",
        features=[
            driver_hourly_stats_fv[["conv_rate"]],  # selecting a single column of driver_hourly_stats_fv
            transformed_conv_rate,
        ]
    )


def driver_activity_v2_fs(
    driver_hourly_stats_fv: FeatureView,
    transformed_conv_rate: OnDemandFeatureView,
) -> FeatureService:
    """Feast definition: grouping of features relative to driver activity"""
    return FeatureService(
        name="driver_activity_v2",
        features=[
            driver_hourly_stats_fv,
            transformed_conv_rate,
        ]
    )


def driver_activity_v3_fs(
    driver_hourly_stats_fresh_fv: FeatureView,
    transformed_conv_rate_fresh: OnDemandFeatureView,
) -> FeatureService:
    """Feast definition: grouping of features relative to driver activity"""
    return FeatureService(
        name="driver_activity_v3",
        features=[
            driver_hourly_stats_fresh_fv,
            transformed_conv_rate_fresh,
        ]
    )


def feast_objects(
    driver_entity: Entity,
    driver_hourly_stats_source: FileSource,
    input_request_source: RequestSource,
    driver_stats_push_source: PushSource,
    driver_hourly_stats_fv: FeatureView,
    driver_hourly_stats_fresh_fv: FeatureView,
    transformed_conv_rate: OnDemandFeatureView,
    transformed_conv_rate_fresh: OnDemandFeatureView,
    driver_activity_v1_fs: FeatureService,
    driver_activity_v2_fs: FeatureService,
    driver_activity_v3_fs: FeatureService,
) -> list:
    """Grouping of the feast definitions to push with with feature_store.apply"""
    return [
        driver_entity,
        driver_hourly_stats_source,
        input_request_source,
        driver_stats_push_source,
        driver_hourly_stats_fv,
        driver_hourly_stats_fresh_fv,
        transformed_conv_rate,
        transformed_conv_rate_fresh,
        driver_activity_v1_fs,
        driver_activity_v2_fs,
        driver_activity_v3_fs,
    ]
