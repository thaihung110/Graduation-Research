import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    date_format,
    dayofmonth,
    dayofweek,
    lit,
    monotonically_increasing_id,
    month,
    quarter,
    regexp_extract,
    round,
    row_number,
    size,
    split,
    to_timestamp,
    when,
    year,
)
from pyspark.sql.window import Window


def transformation_flight():

    # Initialize Spark session
    spark = SparkSession.builder.appName("TransformData").getOrCreate()

    # Define the file path pattern
    file_path_pattern = r"/opt/airflow/data/file_part_1.csv"

    # Load and concatenate all CSV files matching the pattern
    initial_df = spark.read.csv(
        file_path_pattern, header=True, inferSchema=True
    )

    # Select specific columns from the DataFrame
    selected_columns_df = initial_df.select(
        "legId",
        "flightDate",
        "startingAirport",
        "destinationAirport",
        "travelDuration",
        "isBasicEconomy",
        "isRefundable",
        "isNonStop",
        "totalFare",
        "segmentsDepartureTimeRaw",
        "segmentsArrivalTimeRaw",
        "segmentsArrivalAirportCode",
        "segmentsDepartureAirportCode",
        "segmentsAirlineName",
        "segmentsDurationInSeconds",
    )

    print("Finish Selecting Columns")

    # ========================================================
    # Data Cleaning
    # ========================================================

    # Remove duplicates
    cleaned_df = selected_columns_df.dropDuplicates()

    # Handle missing values
    # For example, fill missing values in 'totalFare' with 0 and drop rows with nulls in critical columns
    cleaned_df = cleaned_df.fillna({"totalFare": 0})
    cleaned_df = cleaned_df.dropna(
        subset=["legId", "flightDate", "startingAirport", "destinationAirport"]
    )

    # Convert data types if necessary
    # For example, ensure 'totalFare' is a float
    from pyspark.sql.functions import col

    cleaned_df = cleaned_df.withColumn(
        "totalFare", col("totalFare").cast("float")
    )

    # Extract hours and minutes from the 'travelDuration' column in ISO 8601 format
    hours_pattern = r"PT(\d+)H"
    minutes_pattern = r"(\d+)M"

    # Add new columns for hours and minutes, defaulting to 0 if not present
    cleaned_df = cleaned_df.withColumn(
        "hours",
        coalesce(
            regexp_extract(col("travelDuration"), hours_pattern, 1).cast(
                "float"
            ),
            lit(0.0),
        ),
    )
    cleaned_df = cleaned_df.withColumn(
        "minutes",
        coalesce(
            regexp_extract(col("travelDuration"), minutes_pattern, 1).cast(
                "float"
            ),
            lit(0.0),
        ),
    )

    # Calculate total duration in hours as a float with two decimal places
    cleaned_df = cleaned_df.withColumn(
        "travelDurationHours", round(col("hours") + col("minutes") / 60, 2)
    )

    # Drop the temporary 'hours' and 'minutes' columns
    cleaned_df = cleaned_df.drop("hours", "minutes")

    # Drop the original 'travelDuration' column
    cleaned_df = cleaned_df.drop("travelDuration")

    # Rename 'travelDurationHours' to 'travelDuration'
    cleaned_df = cleaned_df.withColumnRenamed(
        "travelDurationHours", "travelDuration"
    )

    # Convert boolean columns to integer (1 for true, 0 for false)
    cleaned_df = cleaned_df.withColumn(
        "isBasicEconomy", when(col("isBasicEconomy") == True, 1).otherwise(0)
    )
    cleaned_df = cleaned_df.withColumn(
        "isRefundable", when(col("isRefundable") == True, 1).otherwise(0)
    )
    cleaned_df = cleaned_df.withColumn(
        "isNonStop", when(col("isNonStop") == True, 1).otherwise(0)
    )

    # Drop the specified columns from the DataFrame
    cleaned_df = cleaned_df.drop(
        "segmentsArrivalAirportCode",
        "segmentsDepartureAirportCode",
        "segmentsDurationInSeconds",
        "segmentDepartureTimes",
        "segmentArrivalTimes",
    )

    # Convert 'segmentsAirlineName' to an array of strings
    cleaned_df = cleaned_df.withColumn(
        "segmentsAirlineName", split(col("segmentsAirlineName"), r"\|\|")
    )

    # Extract the first departure time and convert it to 'hh:mm AM/PM' format
    cleaned_df = cleaned_df.withColumn(
        "departureTime",
        date_format(
            to_timestamp(
                split(col("segmentsDepartureTimeRaw"), r"\|\|").getItem(0),
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            ),
            "hh:mm a",
        ),
    ).drop("segmentsDepartureTimeRaw")

    # Extract the last arrival time and convert it to 'hh:mm AM/PM' format
    cleaned_df = cleaned_df.withColumn(
        "arrivalTime",
        date_format(
            to_timestamp(
                split(col("segmentsArrivalTimeRaw"), r"\|\|")[
                    size(split(col("segmentsArrivalTimeRaw"), r"\|\|")) - 1
                ],
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            ),
            "hh:mm a",
        ),
    ).drop("segmentsArrivalTimeRaw")

    # Extract the first element from the list in each column and overwrite the existing columns
    cleaned_df = cleaned_df.withColumn(
        "segmentsAirlineName", col("segmentsAirlineName").getItem(0)
    )

    # Overwrite legId with new sequential IDs starting from 0
    cleaned_df = cleaned_df.withColumn(
        "flightKey", monotonically_increasing_id()
    )

    # Increase each flightKey by 1
    cleaned_df = cleaned_df.withColumn("flightKey", col("flightKey") + 1)

    # Drop the old legId column
    cleaned_df = cleaned_df.drop("legId")

    print("Finish Cleaning")

    # ========================================================
    # Finish Cleaning
    # ========================================================

    # ========================================================
    # Airline Dimension
    # ========================================================

    # Define a window specification
    windowSpec = Window.orderBy("airlineName")

    # Select unique airline codes and names
    dimAirline = cleaned_df.select(
        col("segmentsAirlineName").alias("airlineName")
    ).distinct()

    # Add a unique primary key for each airline
    dimAirline = dimAirline.withColumn(
        "airlineKey", row_number().over(windowSpec)
    )

    print("Finish Airline Dimension")

    # ========================================================
    # Finish Airline Dimension
    # ========================================================

    # ========================================================
    # Airport Dimension
    # ========================================================

    # Select unique airport codes from starting and destination airports
    starting_airports = cleaned_df.select(
        col("startingAirport").alias("airportCode")
    )
    destination_airports = cleaned_df.select(
        col("destinationAirport").alias("airportCode")
    )

    # Union the two DataFrames and drop duplicates to get unique airport codes
    all_airports = starting_airports.union(destination_airports).distinct()

    # Create the dimAirport DataFrame
    dimAirport = all_airports.withColumn(
        "airportKey", col("airportCode")
    )  # Assuming airportCode can serve as a unique ID

    # Define a window specification
    windowSpec = Window.orderBy("airportCode")

    # Create the dimAirport DataFrame with a unique primary key
    dimAirport = all_airports.withColumn(
        "airportKey", row_number().over(windowSpec)
    )

    print("Finish Airport Dimension")

    # ========================================================
    # Finish Airport Dimension
    # ========================================================

    # ========================================================
    # Date Dimension
    # ========================================================

    # Select unique flight dates
    dimDate = cleaned_df.select(col("flightDate").alias("date")).distinct()

    # Add additional date-related attributes
    dimDate = (
        dimDate.withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("dayOfMonth", dayofmonth(col("date")))
        .withColumn("dayOfWeek", dayofweek(col("date")))
        .withColumn("quarter", quarter(col("date")))
    )

    # Add a 'dateKey' column by formatting the 'date' column
    dimDate = dimDate.withColumn(
        "dateKey", date_format(col("date"), "yyyyMMdd")
    )

    print("Finish Date Dimension")

    # ========================================================
    # Finish Date Dimension
    # ========================================================

    # ========================================================
    # Fact Table
    # ========================================================

    # Select relevant columns for the fact table
    factFlight = cleaned_df.select(
        col("flightKey"),
        col("flightDate"),
        col("startingAirport").alias("startingAirportId"),
        col("destinationAirport").alias("destinationAirportId"),
        col("segmentsAirlineName").alias(
            "airlineName"
        ),  # Keep original airlineCode for join
        col("isBasicEconomy"),
        col("isRefundable"),
        col("isNonStop"),
        col("totalFare"),
        col("travelDuration"),
        col("departureTime"),
        col("arrivalTime"),
    )

    # Convert 'flightDate' to 'dateKey' format
    factFlight = factFlight.withColumn(
        "dateKey", date_format(col("flightDate"), "yyyyMMdd")
    )

    # Drop the original 'flightDate' column if no longer needed
    factFlight = factFlight.drop("flightDate")

    # Join factFlight with dimAirline to get the airlineKey
    factFlight = factFlight.join(
        dimAirline.select(col("airlineName"), col("airlineKey")),
        factFlight.airlineName == dimAirline.airlineName,
        "left",
    ).drop(
        "airlineName"
    )  # Drop the original airlineCode after join

    factFlight = factFlight.join(
        dimAirport.select(
            col("airportKey").alias("startingAirportKey"), col("airportCode")
        ),
        factFlight.startingAirportId == dimAirport.airportCode,
        "left",
    ).drop("startingAirportId", "airportCode")

    factFlight = factFlight.join(
        dimAirport.select(
            col("airportKey").alias("destinationAirportKey"), col("airportCode")
        ),
        factFlight.destinationAirportId == dimAirport.airportCode,
        "left",
    ).drop("destinationAirportId", "airportCode")

    print("Finish Fact Table")

    # ========================================================
    # Finish Fact Table
    # ========================================================

    # ========================================================
    # Export to CSV
    # ========================================================

    def save_as_single_csv(dataframe, temp_dir, final_csv_path):
        # Ghi dữ liệu ra thư mục tạm thời
        dataframe.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")

        # Tìm tệp CSV trong thư mục tạm thời
        for filename in os.listdir(temp_dir):
            if filename.endswith(".csv"):
                temp_csv_path = os.path.join(temp_dir, filename)
                break

        # Di chuyển và đổi tên tệp CSV
        shutil.move(temp_csv_path, final_csv_path)

        # Xóa thư mục tạm thời
        shutil.rmtree(temp_dir)

    # Lưu dimDate
    save_as_single_csv(
        dimDate,
        r"/opt/airflow/warehouse/temp_dim_date",
        r"/opt/airflow/warehouse/dim_date.csv",
    )

    # Lưu dimAirport
    save_as_single_csv(
        dimAirport,
        r"/opt/airflow/warehouse/temp_dim_airport",
        r"/opt/airflow/warehouse/dim_airport.csv",
    )

    # Lưu factFlight
    save_as_single_csv(
        factFlight,
        r"/opt/airflow/warehouse/temp_fact_flight",
        r"/opt/airflow/warehouse/fact_flight.csv",
    )

    # Lưu dimAirport
    save_as_single_csv(
        dimAirline,
        r"/opt/airflow/warehouse/temp_dim_airline",
        r"/opt/airflow/warehouse/dim_airline.csv",
    )
    # ========================================================
    # Finish Export
    # ========================================================
