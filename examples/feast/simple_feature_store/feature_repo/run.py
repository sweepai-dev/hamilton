from datetime import datetime

import store_definitions
import store_operations
from demo_inputs import ONLINE_ENTITY_ROWS  # noqa: F401
from demo_inputs import ONLINE_FEATURES  # noqa: F401
from demo_inputs import STREAM_EVENT_DF  # noqa: F401
from demo_inputs import HISTORICAL_ENTITY_DF, HISTORICAL_FEATURES

from hamilton import base, driver


def main():
    dr = driver.Driver(
        dict(feast_repository_path=".", feast_config={}),
        store_operations,
        store_definitions,
        adapter=base.SimplePythonGraphAdapter(base.DictResult()),
    )

    final_vars = [
        "apply"
        # "driver_activity_v1_fs",
    ]

    inputs = dict(
        driver_source_path="./data/driver_stats.parquet",
        end_date=datetime.now(),
        entity_df=HISTORICAL_ENTITY_DF,
        historical_features_=HISTORICAL_FEATURES,
        batch=True,
    )

    out = dr.execute(final_vars=final_vars, inputs=inputs)

    # dr.display_all_functions("definitions", {"format": "png"})
    # dr.visualize_execution(final_vars, "exec", {"format": "png"}, inputs=inputs)

    print(out.keys())


if __name__ == "__main__":
    main()
