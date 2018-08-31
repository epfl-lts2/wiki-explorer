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
	print('Dataframe loaded in {} s'.format(time.time() - start_t))
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


def update_target(l,r):
	# Choose r if it exist
    if r:
        return r
    else:
        return l

def update_source(l,r):
	if r:
		return None #kill that node
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
	file_list = [file for file in file_list if 'cor' not in file]
	print(file_list)
	# Handle redirect
	redirect_sdf, file_list = load_file('redirect',file_list)
	#redirect_sdf.persist()

	# Handle pageid
	pageid_sdf,file_list = load_file('pageid',file_list)
	#pageid_sdf.persist()

	# Preprocess the redirect data
	redirect_sdf = redirect_sdf.withColumnRenamed('pageTitle','redirect_target')
	redirect_sdf = redirect_sdf.drop('__index_level_0__')
	pageid_sdf = pageid_sdf.drop('__index_level_0__')
	redirect_sdf = redirect_sdf.join(pageid_sdf,'pageId',how='left')
	redirect_sdf = redirect_sdf.withColumnRenamed('pageTitle','redirect_source')
	redirect_sdf = redirect_sdf.drop('pageId')

	# Handle the pagelinks
	if len(file_list) == 0:
		raise FileNotFoundError(1,'No pagelinks found.')
	link_count = 0
	for filename in file_list:
		file_start_time = time.time()
		print('-------------------')
		print('-------------------')
		print('-------------------')
		print('Opening',filename)
		pagelinks_sdf = read_parquet(filename,sqlContext)
		#nb_links = pagelinks_sdf.count()
		#print('Nb of links:',nb_links)
		pagelinks_sdf = pagelinks_sdf.withColumnRenamed('pageTitle','target')
		pagelinks_sdf = pagelinks_sdf.drop('__index_level_0__')
		# turn source ids to titles
		print('Joining pagelinks with page ids...')
		pagelinks_sdf = pagelinks_sdf.join(pageid_sdf, on='pageId', how='inner')
		pagelinks_sdf = pagelinks_sdf.withColumnRenamed('pageTitle','source')
		pagelinks_sdf = pagelinks_sdf.drop('pageId')
		# joining links and redirects
		print('Joining pagelinks with redirect twice...')
		pagelinks_sdf = pagelinks_sdf.join(redirect_sdf,pagelinks_sdf.target == redirect_sdf.redirect_source,how='left')
		pagelinks_sdf =  pagelinks_sdf.drop('redirect_source')
		pagelinks_sdf =  pagelinks_sdf.withColumnRenamed('redirect_target','fix_target')		
		pagelinks_sdf = pagelinks_sdf.join(redirect_sdf,pagelinks_sdf.source == redirect_sdf.redirect_source,how='left')
		pagelinks_sdf =  pagelinks_sdf.drop('redirect_target')
		
		# updating the links
		# updating targets
		update_t_udf = udf(update_target)
		pagelinks_sdf = pagelinks_sdf.select('source', 'fix_target', update_t_udf('target','fix_target').alias('fixed_target'))
		# updating source
		update_s_udf = udf(update_source)
		pagelinks_sdf = pagelinks_sdf.select('fixed_target', update_s_udf('source','fix_target').alias('fixed_source'))
		# remove redirected nodes (sources )
		pagelinks_sdf.printSchema()
		print('Removing redirect nodes...')
		pagelinks_sdf = pagelinks_sdf.na.drop()
		#nb_wo_r = pagelinks_sdf.count()
		#print('Nb of redirect removed:',nb_links - nb_wo_r)
		# remove duplicates
		print('Removing duplicate edges...')
		plcorr_d_sdf = pagelinks_sdf.distinct()
		#nb_links_unique = plcorr_d_sdf.count()
		#print('Nb of duplicates found:',nb_wo_r - nb_links_unique)
		# remove self-edges
		print('Removing self-edges')
		plcorr_sdf = plcorr_d_sdf.filter(plcorr_d_sdf['fixed_source'] != plcorr_d_sdf['fixed_target'])
		#nb_final = plcorr_sdf.count()
		#print('Nb of self-edges found',nb_links_unique - nb_final)
		#print('Final Nb of edges',nb_final)
		#link_count += nb_final
		# Saving to parquet file
		out_file = filename[:-8] + '_cor.parquet'
		print('Saving to file {} ...'.format(out_file))
		plcorr_sdf.write.parquet(out_file, mode = "overwrite")
		print('File processed in {}'.format(time.time() - file_start_time))		


print('------------------------------------------')
#print('Total number of links',link_count)
print('Total processing time: {} s'.format(time.time() - start_time))
print('------------------------------------------')