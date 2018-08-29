from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf
#from pyspark.sql.types import *
#import pandas as pd
import time
import glob
import os
import argparse

def read_parquet(filename,sqlContext):
	start_t = time.time()
	print('Loading parquet file',filename)
	sparkdf = sqlContext.read.parquet(filename)
	print('Dataframe loaded in {} s'.format(start_t - time.time()))
	return sparkdf


def load_file(name,file_list):
	f = [file for file in file_list if name in file]
	if not f:
		raise FileNotFoundError(1,name + ': file not found.')
	elif len(f)>1:
		raise ValueError('Too many candidates for ' + name + 
			' (found {}). Can only handle one file.'.format(len(redirect)))
	else:
		f_sparkdf = read_parquet(f[0],sqlContext)
		file_list.remove(f[0])
		return f_sparkdf, file_list


def update_col(l,r):
    if r:
        return r
    else:
        return l

def is_dir(dirname):
	"""Checks if a path is an actual directory"""
	if not os.path.isdir(dirname):
		msg = "{0} is not a directory".format(dirname)
		raise argparse.ArgumentTypeError(msg)
	else:
		return dirname

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='Correct links using redirects.')
	parser.add_argument('path', type=is_dir, nargs=1,
					   help='Path where the parquet files are stored.')

	args = parser.parse_args()
	print('Processing', args.path)
	# Path where the file can be found
	path = args.path[0]


	start_time = time.time()
	spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
	# Optimize the convertion of pandas and spark dataframes
	#spark.conf.set("spark.sql.execution.arrow.enabled", "true")
	#spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 1000)
	sqlContext = SQLContext(sparkContext=spark)

	file_list =  glob.glob(os.path.join(path,'*.parquet'))
	print(file_list)
	# Handle redirect
	redirect_sdf, file_list = load_file('redirect',file_list)
	#redirect_sdf.persist()

	# Handle pageid
	pageid_sdf,file_list = load_file('pageid',file_list)
	#pageid_sdf.persist()

	# Preprocess the redirect data
	redirect_sdf = redirect_sdf.withColumnRenamed('pageTitle','fix_target')
	redirect_sdf = redirect_sdf.drop('__index_level_0__')
	pageid_sdf = pageid_sdf.drop('__index_level_0__')
	redirect_sdf = redirect_sdf.join(pageid_sdf,'pageId',how='left')
	redirect_sdf = redirect_sdf.withColumnRenamed('pageTitle','initial_target')
	redirect_sdf = redirect_sdf.drop('pageId')

	# Handle the pagelinks
	if len(file_list) == 0:
		raise FileNotFoundError(1,'No pagelinks found.')
	for filename in file_list:
		pagelinks_sdf = read_parquet(filename,sqlContext)
		pagelinks_sdf = pagelinks_sdf.withColumnRenamed('pageTitle','target')
		pagelinks_sdf = pagelinks_sdf.drop('__index_level_0__')
		# turn source ids to titles
		pagelinks_sdf = pagelinks_sdf.join(pageid_sdf, on='pageId', how='inner')
		pagelinks_sdf = pagelinks_sdf.withColumnRenamed('pageTitle','source')
		pagelinks_sdf = pagelinks_sdf.drop('pageId')
		# joining links and redirects
		pagelinks_sdf = pagelinks_sdf.join(redirect_sdf,pagelinks_sdf.target == redirect_sdf.initial_target,how='left')
		pagelinks_sdf =  pagelinks_sdf.drop('initial_target')
		# updating the links
		update_udf = udf(update_col)
		pagelinks_sdf = pagelinks_sdf.select('source', update_udf('target','fix_target').alias('fix_target'))
		# Saving to parquet file
		pagelinks_sdf.write.parquet(filename[:-7] + 'corrected.parquet')		

print('------------------------------------------')
print('Total processing time: {} s'.format(time.time() - start_time))
print('------------------------------------------')