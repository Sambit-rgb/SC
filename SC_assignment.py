import os
import urllib.request
import ssl
from urllib.error import URLError
from pyspark.sql import SparkSession

# Initialize SparkSession with PostgreSQL connector
spark = SparkSession.builder \
    .appName("JSON Processing") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.24") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# PostgreSQL connection details
postgresql_url = "jdbc:postgresql://localhost:5432/SCdb"
postgresql_properties = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

# Function to fetch and store data
def fetch_and_store_data():
    # Define the URL
    url = 'https://raw.githubusercontent.com/codingo/Ransomware-Json-Dataset/master/ransomware_overview.json'

    try:
        # Create an SSL context to handle SSL certificate verification
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Fetch the JSON data from the URL using urllib.request with SSL context
        with urllib.request.urlopen(url, context=ssl_context) as response:
            data = response.read().decode()

        # Read JSON data into a DataFrame
        df = spark.read.json(spark.sparkContext.parallelize([data]))

        # Check if the version already exists in the database
        existing_versions = spark.read.jdbc(url=postgresql_url, table="public.scdata", properties=postgresql_properties)
        new_versions = df.join(existing_versions, ["Major", "Minor", "Build"], "left_anti")

        # Write new versions to PostgreSQL
        new_versions.write.jdbc(url=postgresql_url, table="public.scdata", mode="append", properties=postgresql_properties)

    except URLError as e:
        print(f"Error fetching data from {url}: {e}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    finally:
        spark.stop()

# Run the function
fetch_and_store_data()
