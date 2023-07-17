# Hamilton + Feast

In this example, we're going to show you how Hamilton can help you structure your Feast repository and bring tighter coupling between your feature preprocessing and feature store.
- **Feast** is a feature store, which an ML-specific stack component, that helps store and serve features from different sources (offline vs. online, batch vs. stream). Features need to be computed separately, typically in an SQL pipeline or a Python dataframe library ([Feast FAQ](https://feast.dev/)). 
- **Hamilton** is a data transformation framework. It helps developer write Python code that is modular and reusable, and that can be executed as a DAG. It was initially developed for large dataframes with hundreds of columns for machine learning while preserving strong lineage capabilities ([high-level comparison](https://hamilton.dagworks.io/en/latest/)).


## File organization
- `/default_feature_store` is the quickstart example you can generate by calling `feast init default`. It is presented here as a reference point to compare with Hamilton + Feast alternatives.
- `/simple_feature_store` is a 1-to-1 reimplementation of `/default_feature_store`. You will notice that adding Hamilton helps explicit the dependencies between Feast objects therefore increasing readability and maintainability.
- `/integration_feature_store` extends the `/simple_feature_store` example by adding the feature preprocessing code using Hamilton and directly integrating with Feast.


## Learn more about Feast
- Hands-on workshop: https://github.com/feast-dev/feast-workshop