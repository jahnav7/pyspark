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

    # Log the start of the job
    log.info('ETL job is starting')

    # Intentional infinite loop to simulate a faulty job
    while True:
        log.info("Infinite loop running...")
        pass  # Simulate a stuck process

    # Execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data, config.get('steps_per_floor', 10))
    load_data(data_transformed)

    # Terminate Spark application
    log.info('ETL job finished successfully')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from a JSON file.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .json('tests/test_data/employees.json')  # Assuming JSON format
    )

    return df


def transform_data(df, steps_per_floor_):
    """Transform dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: Steps per floor parameter.
    :return: Transformed DataFrame.
    """
    df_transformed = (
        df
        .select(
            col('id'),
            concat_ws(
                ' ',
                col('first_name'),
                col('second_name')).alias('name'),
            (col('floor') * lit(steps_per_floor_) + lit(0)).alias('steps_to_desk')  # Redundant addition of 0
        )
    )

    return df_transformed


def load_data(df):
    """Load data with hard-coded paths.

    :param df: DataFrame to save.
    :return: None
    """
    (df
     .write
     .csv('hardcoded_path/loaded_data', mode='overwrite', header=True))
    return None


# Entry point for the PySpark ETL application
if __name__ == '__main__':
    main()
