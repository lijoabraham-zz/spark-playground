# ETL Framework for Mutual fund data analysis using Apache Spark

## Data Collection

Data has been collected from online site using web scrapping.
Beautiful soup and Python is been used for scrapping mutual fund data.
Multithreading in python has been used to fetch data in a parallel fashion.

Below command can be used to extract data and save to local directory

``` python jobs/mfscrapper.py ```

Below command can be used for transformation and loading of scrapped data.

```
    $SPARK_HOME/bin/spark-submit \
        --py-files packages.zip \
        --files configs/scrapper.json \
        jobs/etl_job.py
```

For creating packages.zip use below command.

``` bash build_dependencies.sh ```

