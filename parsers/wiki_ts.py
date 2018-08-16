

import bz2
import os
import pandas
import codecs
import re
from tqdm import tqdm
import pandas as pd

def read_data(filename):
	wrapper = codecs.getreader('utf-8')
	#linefilter = re.compile("(\S+)")
	print('Extracting data from',filename)	
	with bz2.open(filename, 'rb') as f:
		wf = wrapper(f,errors='replace')
		#count = 0
		page_list = []
		for line in tqdm(wf):
			#count += 1
			#info_list = linefilter.findall(line)
			#info_list = line.split()
			#if info_list[0] == 'en.z': # english wikipedia prefix
			#	page_list.append(info_list[1:])
			if line.startswith('en.z'): # english wikipedia prefix
				info_list = line.split()
				page_list.append(info_list[1:])
	print('Converting to Dataframe')
	df=pd.DataFrame(page_list,columns=['Title','DailyTotal','EncodedTimeseries'])
	df.DailyTotal = df.DailyTotal.astype(int)
	return df



def decode_ts(df,visit_threshold):
	# df the dataframe of encoded timeseries
	# visit_threshold : minimal number of visits during the day to be selected
	print('Decoding timeseries')
	ts_df = pandas.DataFrame(dtype=int)
	for row in tqdm(df.itertuples()):
		if int(row.DailyTotal) >= visit_threshold:
			row_idx = row.Index
			# The encoded timeseries contains pieces
			# starting with one letter (hour of the day), followed by a number (visits) 
			hourly_sliced = re.findall('\w\d+',row.EncodedTimeseries)
			for hour_slice in hourly_sliced:
				ts_df.loc[row_idx, hour_slice[0]] = int(hour_slice[1:])
	ts_df = ts_df.fillna(0).astype(int)
	ts_df.sort_index(axis=1, inplace=True)
	return ts_df

def merge_pages_and_ts(df,df_ts):
	result = pandas.merge(df.drop(columns='EncodedTimeseries'), df_ts, how='right', left_index=True, right_index=True)
	result = result.sort_values('DailyTotal', ascending=False)
	return result

def get_info_from_filename(filename):
	folder,fname = os.path.split(filename)
	date = fname[11:-4]
	#print('Date of file:',date)
	return folder, fname, date

def extract_timeseries(filename,min_visits_per_day):
	data_df = read_data(filename)
	df_ts = decode_ts(data_df,min_visits_per_day)
	full_data = merge_pages_and_ts(data_df,df_ts)
	# Saving to csv file
	folder, fname, date = get_info_from_filename(filename)
	csv_file = 'timeseries-' + date + '-T' + str(min_visits_per_day) + '.csv'
	csv_path = os.path.join(folder,csv_file)
	full_data.to_csv(csv_path,index=False)
	return full_data

if __name__ == "__main__":
	print('Module for extracting the number of visits per page per hour'
		+ ' recorded by Wikimedia')