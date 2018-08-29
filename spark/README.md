## Spark processing

In order to process the large amount of data from the Wikipedia dumps, several Python scripts using spark have been written.

The file `remove_redirects.py` combine the data from parsed parquet files to generate a list of files containing the links with the redirect nodes removed.

In order to run the python script, the bash file `run_script.sh` may be used. It avoid conficts when pyspark is configured to be used with jupyter notebooks.
the command line is the following:
```
./run_script.sh remove_redirects.py path
```
or 
```
pyspark remove_redirects.py path
```
The `path` is the folder where the parquet data files are located.


Several Jupyter notebooks are also available. They are used to test the processing and check if it works correctly. 

