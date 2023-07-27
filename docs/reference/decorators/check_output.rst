=======================
check_output*
=======================
The ``@check_output`` decorator enables you to add simple data quality checks to your code.

For example:

.. code-block:: python

    import pandas as pd
    import numpy as np
    from hamilton.function_modifiers import check_output

    @check_output(
        data_type=np.int64,
        data_in_range=(0,100),
    )
    def some_int_data_between_0_and_100() -> pd.Series:
        pass

The check\_output validator takes in arguments that each correspond to one of the default validators. These arguments
tell it to add the default validator to the list. The above thus creates two validators, one that checks the datatype
of the series, and one that checks whether the data is in a certain range.

Note that you can also specify custom decorators using the ``@check_output_custom`` decorator.

The ``@check_output_custom`` decorator allows you to implement your own custom validators. It takes in one or more custom validators that implement the `DataValidator` interface. It also takes an optional `target_` parameter that specifies the nodes to check the output of.

Here is an example of how to use the ``@check_output_custom`` decorator:

.. code-block:: python

    from hamilton.function_modifiers import check_output_custom
    from tests.resources.dq_dummy_examples import SampleDataValidator2, SampleDataValidator3

    @check_output_custom(
        SampleDataValidator2(dataset_length=1, importance="warn"),
        SampleDataValidator3(dtype=np.int64, importance="warn"),
    )
    def some_function(input: pd.Series) -> pd.Series:
        return input

In this example, the `check_output_custom` decorator is used with two custom validators, `SampleDataValidator2` and `SampleDataValidator3`. The `importance` parameter is set to "warn" for both validators.

See `data_quality <https://github.com/dagworks-inc/hamilton/blob/main/data\_quality.md>`_ for more information on
available validators and how to build custom ones.

----

**Reference Documentation**

.. autoclass:: hamilton.function_modifiers.check_output
   :special-members: __init__

.. autoclass:: hamilton.function_modifiers.check_output_custom
   :special-members: __init__
