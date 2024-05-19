# SC
SC_assignement
# Required Modules:
The code starts by importing necessary modules:
os: Provides operating system-related functions.
urllib.request: Used for fetching data from a URL.
ssl: Handles SSL certificate verification.
URLError: Represents errors related to URL handling.
SparkSession from pyspark.sql: Initializes a Spark session for data processing.

# Initializing SparkSession:
A Spark session (SparkSession) is created with the following configurations:
appName: Sets the name of the Spark application to “JSON Processing.”
config: Specifies additional configuration properties, including the PostgreSQL connector package version.
getOrCreate(): Retrieves an existing Spark session or creates a new one if none exists.
The log level is set to “ERROR” to suppress unnecessary log messages.

# PostgreSQL Connection Details:
The PostgreSQL connection details are defined:
postgresql_url: The JDBC URL for connecting to the PostgreSQL database.
postgresql_properties: A dictionary containing connection properties (username, password, and driver).
Fetching and Storing Data:
The fetch_and_store_data function performs the following tasks:
Defines the URL pointing to the JSON data.
Creates an SSL context to handle SSL certificate verification.
Fetches the JSON data from the specified URL using urllib.request.
Reads the JSON data into a Spark DataFrame (df).
Retrieves existing versions from the PostgreSQL database.
Performs a left anti-join to get only the new versions (not already present in the database).
Writes the new versions to the PostgreSQL table named “public.scdata” in append mode.

# Error Handling:
The code includes exception handling for URLError and other unexpected exceptions.
If an error occurs during data fetching or processing, an appropriate message is printed.
Stopping Spark Session:
The Spark session is stopped in the finally block to release resources.
Running the Function:
The fetch_and_store_data function is called to execute the data fetching and storage process.
