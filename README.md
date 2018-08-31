# Tools for the Wikipedia datasets

This repository provides tools for the analysis of Wikipedia data. 

more info on the data can be found here [https://meta.wikimedia.org/wiki/Data_dumps](https://meta.wikimedia.org/wiki/Data_dumps).

## Wikipedia data parsers

In the folder `parsers/` you will find parsers for the different data files provided by Wikipedia.


## Building a Wikipedia Graph from the page hyperlinks

### Handling redirects

The `pagelinks` file provided by wikipedia contains all the hyperlinks between articles including the redirects links. 

In order to build the graph without these redirects, one can use the method provided in the parsers or using Spark.

**With Python only**

```
python3 parsers/linkparser path combine parquet
```
where "path" is the folder where to find the dumps,

**With Spark** 

The first step is to parse the data using 
```
python3 parsers/linkparser path all parquet
```
where "path" is the folder where to find the dumps
the second step is to call the spark program
```
spark/pyspark remove_redirects.py path
```
The `path` is the folder where the parquet data files are located.

See the `spark/` and `parsers/` readme files for more details.

## Monitoring Wikipedia visits activity

To do

## License

The code is open-source under the [Apache v2 license](https://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018 Laboratoire de traitement du signal 2, EPFL