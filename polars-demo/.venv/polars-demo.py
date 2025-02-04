import polars as pl
import pandas as pd
import time

#POLARS

# Parquet file with 1 million flight records read using polars
polarsDf = pl.read_parquet("../Resources/flights-1m.parquet")


# Filtered data where flight distance is greater than 1500
polarsDistanceTravelledFilterDf = polarsDf.filter(pl.col('DISTANCE') > 1500)

print(polarsDistanceTravelledFilterDf)


start_time_polars = time.time()

#  Polars Flight Dataframe converted into a newline delimited JSON file(Have to create an empty json file before this operation)
dataFrameToJson = polarsDistanceTravelledFilterDf.write_ndjson("../Resources/filteredFlightsDataPolars.json")

# Calculate time taken to convert filtered dataset to JSON for polars
end_time_polars = time.time()
print("Time Taken to Print Data:", end_time_polars-start_time_polars, "Seconds")


# PANDAS

# Parquet file with 1 million flight records read using pandas
pandasDF = pd.read_parquet("../Resources/flights-1m.parquet", engine ='auto')

# Filtered data where flight distance is greater than 1500
pandasDistanceTravelledFilterDf = pandasDF[pandasDF['DISTANCE'] > 1500]

print(pandasDistanceTravelledFilterDf)

startTimePandas = time.time()

#  Pandas Flight Dataframe converted into a JSON file(Automatically creates a new JSON file at the filepath)
convertPandasDFToJson = pandasDistanceTravelledFilterDf.to_json('../Resources/filteredFlightsDataPandas.json', orient='records', lines=True)

# Calculate time taken to convert filtered dataset to JSON for pandas
endTimePandas = time.time()
print("Time Taken to Print Data:", endTimePandas-startTimePandas, "Seconds")


# In this instance Polars is approximately 5x faster than Pandas when converting the filtered dataframe to json.
# Have to take into consideration that the json file was pre-made for polars
# while pandas creates the file when converting to json

