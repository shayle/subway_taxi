Project quantifying the effect of subway delays on taxi ridership

taxi_categorizing.py: To be run on HPC cluster. Script uses PySpark to read in taxi data for June 2016. Assigns each trip to an date, hour and subway station if within 300 foot radius. Points not adjacent are dropped. Returns csv with containing a ride count for each date, time and station. 

Ridership_with_Delay.ipynb: Merges taxi data with subway delay data and generates plots + runs ttest.  


