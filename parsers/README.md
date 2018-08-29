## Extraction of time series of visits per page per hour

Data are available at [https://dumps.wikimedia.org/other/pagecounts-ez/](https://dumps.wikimedia.org/other/pagecounts-ez/)

The module is named `wiki_ts`.

## Extraction of data from the dumps

The dumps can be found here [https://dumps.wikimedia.org/](https://dumps.wikimedia.org/) and here for the english version [https://dumps.wikimedia.org/enwiki/](https://dumps.wikimedia.org/enwiki/).

3 `sql.gz` files are needed to create the wikipedia graph of hyperlinks: 

* the `page` file containing the id and corresponding title of pages (among other information),
* the `pagelinks` file containing the list of hyperlinks written as source id and target title,
* the `redirect` file listing the redirect links. It is organized in the following manner: source page id (page that is redirected) , target page title (page that receives the redirected visitors).

The extraction is performed by `linkparser.py`. It can be called with the following command
```
python3 linkparser.py path type outfile_type
```
* `path` is the folder where to find the 'gz'-compressed sql dumps.
* `type`, is the type of processing, it can be "pageid", "redirect", "pagelinks", "all" or "combine". For the first 3 choices, the program will parse only the desired file. "all" will parse all 3 files and "combine" will also combine the files to remove the redirects and replace page ids by their titles. "combine" use the already parsed files if they exist otherwise it parses them from the dump.
* `outfile_type` is either "gz" or "parquet" and is the file type outputed by the program.



