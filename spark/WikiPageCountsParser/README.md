# Wikipedia page-counts parser

Raw page-counts files can be downloaded from Wikimedia dumps: https://dumps.wikimedia.org/other/pagecounts-ez/merged/

To parse page-counts:
1. Put downloaded files (pagecounts-YEAR-MONTH-DAY.bz2) to a sub-folder (e.g. ```jan2018``` for January 2018 page-counts) in the ```resources``` folder in ```WikiPageCountsParser/src/main/resources/```.
2. Go to [Globals.scala](https://github.com/epfl-lts2/wiki-explorer/blob/master/spark/WikiPageCountsParser/src/main/scala/ch/epfl/lts2/Globals.scala) and change ```PATH_RESOURCES``` to the location of the ```resources``` folder on your computer.
3. Go to [WikiPageCountsParser.scala](https://github.com/epfl-lts2/wiki-explorer/blob/master/spark/WikiPageCountsParser/src/main/scala/WikiPageCountsParser.scala) and change ```YEAR```, ```MONTH```, and ```DAYS``` constants according to the files you have in the ```resources``` folder.
4. In the same file, set Wikimedia ```PROJECT``` code (e.g., "en.z" for English Wikipedia).
5. In the same file, set ```DAILY_THRESHOLD``` to exclude pages whose daily activity below the threshold.
6. In the same file, ```FOLDER``` corresponds to a sub-folder name (e.g. ```jan2018```) in the ```resources```, where you placed raw page-counts. The same name will be used for parsed output files (e.g. ```jan2018.parquet```).
