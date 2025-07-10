# Flight Delay Analysis Using Big Data Processing with MapReduce

This project analyzes U.S. domestic flight delays and taxi times using Hadoop MapReduce on a real-world dataset with millions of flight records. It demonstrates how big data techniques can be applied to gain insights into airline performance and airport efficiency.


## Project Overview

Using two custom MapReduce jobs written in Java, this project:
- Calculates **on-time performance rates** for different airlines
- Computes the **average taxi-in and taxi-out times** for each airport

Both jobs were deployed on a Hadoop cluster set up on AWS EC2 instances.


## Dataset

- **Name**: Flight Delay and Cancellation Dataset (2019–2023)  
- **Source**: [Kaggle - Flight Delay Dataset](https://www.kaggle.com/datasets/patrickzel/flight-delay-and-cancellation-dataset-2019-2023)  
- **Size**: 3M+ records  
- **Format**: CSV  
- **Features Used**: Airline codes, delay reasons, origin/destination airports, taxi times, cancellation flags, etc.


## Technologies Used

- **Java** (MapReduce programming)
- **Apache Hadoop** (MapReduce framework)
- **AWS EC2** (Ubuntu-based cluster setup)
- **HDFS** (for distributed file storage)
- **Linux Shell** (compilation and job execution scripts)


## MapReduce Programs

### 1️. OnTimePerformance.java

- **Mapper**: Parses airline code and calculates total delay (carrier + weather + NAS + security + late aircraft). Flags flight as on-time if total delay ≤ 5 minutes.
- **Reducer**: Calculates the on-time rate percentage for each airline.


### 2️. TaxiTimeAnalysis.java

- **Mapper**: Emits `(airport_code, taxi_time)` pairs from TAXI_OUT (origin) and TAXI_IN (destination).
- **Reducer**: Computes average taxi time per airport.


## Execute

hadoop jar ontimeperf.jar OnTimePerformance /flightdata_small/flights_small_cleaned.csv /flightdata_small/ontime_output

hadoop jar taxitime.jar TaxiTimeAnalysis /flightdata_small/flights_small_cleaned.csv /flightdata_small/taxitime_output

## Sample outputs

### OnTimePerformance

DL  TotalFlights=500, OnTimeFlights=420, OnTimeRate=84.00%

UA  TotalFlights=450, OnTimeFlights=360, OnTimeRate=80.00%

### TaxiTimeAnalysis

JFK  AvgTaxiTime: 21.3

ORD  AvgTaxiTime: 18.9

## Challenges Faced

- Dataset size (3M records) was too large for basic EC2; used sampling (~50K rows)
- Missing values in delay and taxi columns required careful cleaning
- Hadoop required proper output folder handling to avoid overwrite errors


## Future Improvements

- Use Apache Spark for faster in-memory processing
- Analyze full dataset with optimized EC2 clusters
- Add visualizations with Power BI / Tableau


## Author

**Tanushri Vijayakumar**
tanushri.fall24@gmail.com
