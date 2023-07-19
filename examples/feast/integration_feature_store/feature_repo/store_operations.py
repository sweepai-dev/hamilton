from datetime import datetime

import pandas as pd
from feast import FeatureStore, FeatureService
from feast.data_source import PushMode, PushSource
from feast.diff.infra_diff import InfraDiff
from feast.diff.registry_diff import RegistryDiff
from feast.infra.infra_object import Infra
from feast.repo_contents import RepoContents

from hamilton.function_modifiers import config, extract_fields


def feature_store(feast_repository_path: str, feast_config: dict) -> FeatureStore:
    """Instantiate Feast core object, the FeatureStore"""
    if feast_config:
        return FeatureStore(repo_path=feast_repository_path, config=feast_config)
    else:
        return FeatureStore(repo_path=feast_repository_path)


@extract_fields(dict(
    registry_diff=RegistryDiff,
    infra_diff=InfraDiff,
    new_infra=Infra,
    diff_string=str,
))
def plan(feature_store: FeatureStore, feast_repository_content: RepoContents) -> dict:
    """Diff between the current repository state and `feast_repository_content`
    No changes are applied
    """
    registry_diff, infra_diff, new_infra = feature_store.plan(feast_repository_content)
    return dict(
        registry_diff=registry_diff, 
        infra_diff=infra_diff, 
        new_infra=new_infra,
    )


def apply(
    feature_store: FeatureStore,
    registry_diff: RegistryDiff,
    infra_diff: InfraDiff,
    new_infra: Infra,
) -> str:
    """Update to `feast_repository_content` by applying the registry diffs"""
    feature_store._apply_diffs(registry_diff, infra_diff, new_infra)
    applied_diff_string = registry_diff.to_string()
    print(applied_diff_string)
    return applied_diff_string


def materialize_incremental(feature_store: FeatureStore, end_date: datetime) -> bool:
    """Loads data from offline store to online store, up to end_date
    Has side-effect only; returns a boolean for lineage
    """
    feature_store.materialize_incremental(end_date=end_date)
    return True


def push(
    feature_store: FeatureStore,
    push_source: str,
    event_df: pd.DataFrame,
    push_mode: PushMode | int
) -> bool:
    """Push features to a push source; updates all features associated with this source
    Has side-effect only; returns a boolean for lineage
    """
    if isinstance(push_mode, int):
        push_mode = PushMode(push_mode)
    feature_store.push(push_source, event_df, to=push_mode)
    return True


@config.when_not(batch_scoring=True)
def historical_features__not_batch(
    feature_store: FeatureStore,
    entity_df: pd.DataFrame,
    historical_features_: list[str] | FeatureService,
) -> pd.DataFrame:
    """Retrieves point-in-time correct historical feature for the specified entities"""
    return feature_store.get_historical_features(
        entity_df=entity_df,
        features=historical_features_,
    ).to_df()


@config.when(batch_scoring=True)
def historical_features__batch(
    feature_store: FeatureStore,
    entity_df: pd.DataFrame,
    entity_timestamp_col: str,
    historical_features_: list[str] | FeatureService,
) -> pd.DataFrame:
    """Retrieves all historical feature for the specified entities
    Setting all timestamp to now() allows to retrieve all rows
    """
    # For batch scoring, we want the latest timestamps
    entity_df[entity_timestamp_col] = pd.to_datetime("now", utc=True)
    return feature_store.get_historical_features(
        entity_df=entity_df,
        features=historical_features_,
    ).to_df()


def online_features(
    feature_store: FeatureStore,
    entity_rows: list[dict],
    online_features_: list[str] | FeatureService | PushSource 
) -> pd.DataFrame:
    """Fetch online features from a FeatureService source"""
    return feature_store.get_online_features(
        entity_rows=entity_rows,
        features=online_features_,
    ).to_df()
