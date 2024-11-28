"""
etl_job_faulty.py
~~~~~~~~~~~~~~~~~

This Python module contains an ETL job with intentional faults for
demonstration purposes.
"""

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql import DataFrame  # Unused import

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # Start Spark application and get Spark session, logger, and config
    spark, log, config = start_spark(
        app_name='my_faulty_etl_job', files=['configs/etl_config.json'])

    # Log the start of the job with inconsistent level and typo
    log.information('ETL job is strating')  # Typo and wrong log level

    # Execute ETL pipeline
    try:
        data = extract_data(spark)
        data_transformed = transform_data(data, config.get('steps_per_floor', 10))
        load_data(data_transformed)
    except Exception as e:
        log.warn(f"Job failed due to: {e}")  # Using deprecated logging method
        raise e

    # Terminate Spark application without proper cleanup
    log.info('ETL job finished without cleanup')
    spark.stop()  # Missing cleanup for intermediate files
    return None


def extract_data(spark):
    """Load data from an unsupported file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    # Using an incorrect path and unsupported file format
    df = (
        spark
        .read
        .text('tests/test_data/employees.json')  # Incorrect format for the data
    )

    # Not validating schema
    return df


def transform_data(df, steps_per_floor_):
    """Transform dataset with unnecessary computations and assumptions.

    :param df: Input DataFrame.
    :param steps_per_floor_: Steps per floor parameter.
    :return: Transformed DataFrame.
    """
    # Assuming column existence without validation
    df_transformed = (
        df
        .select(
            col('id'),
            concat_ws(
                ' ',
                col('first_name'),  # Assuming this column always exists
                col('second_name')).alias('name'),
            (col('floor') * lit(steps_per_floor_) + lit(0) - lit(None)).alias('steps_to_desk')  # Invalid operation
        )
    )

    return df_transformed


def load_data(df):
    """Load data with hardcoded paths and inefficient operations.

    :param df: DataFrame to save.
    :return: None
    """
    # Inefficient save operation: Repartitioning unnecessarily
    (df
     .repartition(100)  # Excessive number of partitions
     .write
     .csv('hardcoded_path/loaded_data', mode='overwrite', header=True))  # Hardcoded path
    return None


def create_test_data(spark):
    """Create faulty test data.

    :return: None
    """
    # Create example data with schema inconsistencies
    local_records = [
        Row(id=1, first_name='Dan', floor=1),  # Missing 'second_name'
        Row(id=2, first_name=None, second_name='Sommerville', floor=1),  # Null value in first_name
        Row(id=3, first_name='Alex', second_name='Ioannides', floor='two'),  # Incorrect data type for floor
    ]

    df = spark.createDataFrame(local_records)

    # Save data
    (df
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees', mode='overwrite'))

    return None


# Entry point for the PySpark ETL application
if __name__ == '__main__':
    main()
