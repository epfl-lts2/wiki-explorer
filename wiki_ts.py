

import bz2
import os
import pandas
import codecs
import re
from tqdm import tqdm
import pandas as pd

def read_data(filename):
	wrapper = codecs.getreader('utf-8')
	linefilter = re.compile("(\S+)")
	print('Extracting data from',filename)	
	with bz2.open(filename, 'rb') as f:
		wf = wrapper(f,errors='replace')
		count = 0
		page_list = []
		for line in tqdm(wf):
			count += 1
			info_list = linefilter.findall(line)
			if info_list[0] == 'en.z': # english wikipedia prefix
				page_list.append(info_list[1:])
				#print(line)
				#print(info_list)
			#if (count % 100000) == 0:
			#    print(count)
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


if __name__ == "__main__":
	print('Module for extracting the number of visits per page per hour recorded by Wikimedia')