
import os
import codecs
import re
from tqdm import tqdm
import pandas
import time
import gzip
import argparse
import glob

pagelinksR = re.compile("\((\d+),"      # pl_from             (source page id)
						 + "(\d+),"        # page_namespace      (namespace number)
						 + "'(.*?)',"      # page_title          (page title w/o namespace)
						 + "(\d+)\)"       # pl_from_ns          (source page namespace)
)

redirectR = re.compile("\((\d+),"      # rd_from (page ID number)
						 + "(\d+),"        # rd_namespace
						 + "'(.*?)',"      # rd_title
						 + "(.*?),"        # rd_interwiki
						 + "(.*?)\)"       # rd_fragment
		)

pageidR = re.compile("\((\d+),"      # page_id             (page ID number)
						 + "(\d+),"        # page_namespace      (namespace number)
						 + "'(.*?)',"      # page_title          (page title w/o namespace)
						 + "'(.*?)',"      # page_restrictions   (protected pages have 'sysop' here)
						 + "(\d+),"        # page_counter        (view counter, disabled on WP)
						 + "([01]),"       # page_is_redirect
						 + "([01]),"       # page_is_new
						 + "([\d\.]+?),"   # page_random         (for random page function)
						 + "'(\d{14})',"   # page_touched        (cache update timestamp)
						 + "(.*?),"        # page_links updated (timestamp or null)
						 + "(\d+),"        # page_latest      (namespace number)
						 + "(\d+),"        # page_len      (namespace number)
						 + "(.*?)\)"       # page_content_model          (page content model)
		)

def reduce2ns_pagelinks(data_list,namespace):
	namespace = str(namespace)
	return [(info[0],info[2]) for info in data_list if (info[1] == namespace and info[3] == namespace)]
def reduce2ns_redirect(data_list,namespace):
	namespace = str(namespace)
	return [(info[0],info[2]) for info in data_list if (info[1] == namespace)]
def reduce2ns_page(data_list,namespace):
	namespace = str(namespace)
	return [(info[0],info[2]) for info in data_list if (info[1] == namespace)]

def file2df(file,regexparser,reduce2ns,ns):
	print('Extracting data from ',file)
	start = time.time()
	wrapper = codecs.getreader('utf-8')
	with gzip.open(file, 'rb') as f:
		wf = wrapper(f,errors='replace')
		#wf = f
		count = 0
		pageid_list = []
		for line in tqdm(wf):
			count += 1
			info_list = regexparser.findall(line)
			#print('Length',len(info_list))
			valid_pages = reduce2ns(info_list,ns)
			if valid_pages:
				pageid_list += valid_pages
			#if (count % 100) == 0:
			#	print(count)
	duration = time.time() - start
	print('time: {} min {} s.'.format(int(duration/60),int(duration%60)))
	print('Lines processed', count)
	return pageid_list


# def extract_data_sliced(file,outname,regexparser,reduce2ns,ns,slice_size=1000,dFrame=False):
# 	print('Extracting data from ',file)
# 	folder,filename = os.path.split(input_file)
# 	start_total = time.time()
# 	wrapper = codecs.getreader('utf-8')
# 	with gzip.open(file, 'rb') as f:
# 		wf = wrapper(f,errors='replace')
# 		count = 0
# 		nb_outfile = 0
# 		total_nb_entries = 0
# 		pageid_list = []
# 		start_load = time.time()
# 		# Iterate over the lines of the file
# 		for line in tqdm(wf):
# 			count += 1
# 			info_list = regexparser.findall(line)
# 			valid_pages = reduce2ns(info_list,ns)
# 			if valid_pages:
# 				pageid_list += valid_pages
# 			# For big files, cut in chunks and save to disk to preserve the memory
# 			# For big file, slice_size > 0 
# 			if slice_size != 0 and (count % slice_size) == 0:
# 				load_duration = time.time() - start_load
# 				print('\n Loading time: {} min {} s.'.format(int(load_duration/60),
# 					int(load_duration%60)))
# 				if not pageid_list :
# 					continue
# 				outfile = os.path.join(folder,outname + str(nb_outfile) + '.gz')
# 				nb_entries, df = save_list_as_df(pageid_list,outfile)
# 				nb_outfile +=1
# 				total_nb_entries += nb_entries
# 				# Free some space
# 				pageid_list = []
# 				df = pandas.DataFrame()
# 				start_load = time.time()
# 		# Save the last batch of data
# 		if pageid_list:
# 			outfile = os.path.join(folder,outname + str(nb_outfile) + '.gz')
# 			nb_entries, df = save_list_as_df(pageid_list,outfile)
# 			total_nb_entries += nb_entries
# 	# Concluding information
# 	duration = time.time() - start_total
# 	print('Total time: {} min {} s.'.format(int(duration/60),int(duration%60)))
# 	print('Total lines processed', count)
# 	print('Total number of entries',total_nb_entries)
# 	return df

def extract_data_sliced(file,outname,regexparser,reduce2ns,ns,slice_size,combine=None):
	print('Extracting data from ',file)
	folder,filename = os.path.split(file)
	start_total = time.time()
	nb_outfile = 0
	total_nb_entries = 0
	start_load = time.time()
	for data_slice,line_count in data_slice_generator(file,regexparser,reduce2ns,ns,slice_size):
		outfile = os.path.join(folder,outname + str(nb_outfile) + '.gz')
		nb_entries = len(data_slice)
		if nb_entries > 0 :
			load_duration = time.time() - start_load
			print('Loading time: {} min {} s.'.format(int(load_duration/60),
					int(load_duration%60)))
			print('Nb of entries:',nb_entries)
			save_to_disk(data_slice,outfile)
			total_nb_entries += nb_entries
			if combine != None:
				out_combine = os.path.join(folder,combine[2] + nb_outfile + 'npz')
				data_slice = combine_info(combine[0],combine[1],data_slice,out_combine)
			nb_outfile += 1
			start_load = time.time()
	# Concluding information
	duration = time.time() - start_total
	print('Total time: {} min {} s.'.format(int(duration/60),int(duration%60)))
	print('Total lines processed', line_count)
	print('Total number of entries',total_nb_entries)
	return data_slice


def data_slice_generator(file,regexparser,reduce2ns,ns,slice_size):
	wrapper = codecs.getreader('utf-8')
	with gzip.open(file, 'rb') as f:
		wf = wrapper(f,errors='replace')
		count = 0
		pageid_list = []
		# Iterate over the lines of the file
		for line in tqdm(wf):
			df = pandas.DataFrame() # Free some memory during the iterating process
			count += 1
			info_list = regexparser.findall(line)
			valid_pages = reduce2ns(info_list,ns)
			if valid_pages:
				pageid_list += valid_pages
			# For big files, cut in chunks and save to disk to preserve the memory
			# For big file, slice_size > 0 
			if slice_size != 0 and (count % slice_size) == 0:
				if not pageid_list :
					continue
				print('Building dataFrame...')
				df=pandas.DataFrame(pageid_list,columns=['pageId','pageTitle'])
				df.pageId = df.pageId.astype(int)
				print('dataFrame built.')
				pageid_list = []
				yield df, count
	df=pandas.DataFrame(pageid_list,columns=['pageId','pageTitle'])
	df.pageId = df.pageId.astype(int)
	yield df, count



def save_to_disk(item_df,outfile):
	print('Saving dataframe to ',outfile)
	start_gz = time.time()
	item_df.to_pickle(outfile)
	duration = time.time() - start_gz
	print('time to compress and save: {} min {} s.'.format(int(duration/60),int(duration%60)))
	print('')
	return 0


def process_file(input_file,combine=None):
	folder,filename = os.path.split(input_file)
	if 'pagelinks' in filename:
		print('Extracting links')
		regex_string = pagelinksR
		data_filter = reduce2ns_pagelinks
		outname = 'pagelinks_parsed'
		slice_size = 500
	elif 'page' in filename:
		print('Extracting pages ids and titles')
		regex_string = pageidR
		data_filter = reduce2ns_page
		slice_size = 0
		outname = 'pageid_parsed'
	elif 'redirect' in filename:
		print('Extracting redirects')
		regex_string = redirectR
		data_filter = reduce2ns_redirect
		outname = 'redirect_parsed'
		slice_size = 0
	else:
		print('Wrong type of file.')
		raise ValueError('Can not process this type of file.')


	output_df = extract_data_sliced(input_file,outname,regex_string,data_filter,'0',slice_size,combine)
	return output_df

def combine_info(redirect_df,pageid_df,pagelinks_df,outfilename):
	# Translate id to title
	pagelinks_df.rename(columns={'pageTitle' : 'target'}, inplace=True)
	pagelinks_df = pagelinks_df.join(pageid_df, on='pageId', how='inner')
	pagelinks_df.rename(columns={'pageTitle' : 'source'}, inplace=True)
	# Replace targets using redirects
	#pagelinks_df.rename(columns={'pageId' : 'sourceId'}, inplace=True)
	pagelinks_df = pagelinks_df.drop('pageId', axis=1)
	pagelinks_df['fix_target'] = pagelinks_df['target'].copy()
	pagelinks_df = pagelinks_df.set_index('target')
	pagelinks_df.update(redirect_df)
	pagelinks_df.reset_index(inplace = True)
	# Label the redirected
	pagelinks_df['is_redirect'] = (pagelinks_df['target'] != pagelinks_df['fix_target'])*1
	pagelinks_df = pagelinks_df.reindex(columns=['source','target','fix_target','is_redirect'])
	save_to_disk(pagelinks_df,outfilename)
	return pagelinks_df

def df_reshape(redirect_df,pageid_df):
	print('Re-arranging redirect and pageId dataframes...')
	# Prepare the page list
	pageid_df.set_index('pageId', inplace=True)
	# Prepare the redirect list
	redirect_df.rename(columns={'pageId' : 'initial_id', 'pageTitle' : 'fix_target'}, inplace=True)
	redirect_df = redirect_df.join(pageid_df, on='initial_id', how='inner')
	redirect_df.rename(columns={'pageTitle' : 'initial_target'}, inplace=True)
	redirect_df = redirect_df.drop('initial_id', axis=1)
	redirect_df = redirect_df.set_index('initial_target')
	return redirect_df,pageid_df

def is_dir(dirname):
	"""Checks if a path is an actual directory"""
	if not os.path.isdir(dirname):
		msg = "{0} is not a directory".format(dirname)
		raise argparse.ArgumentTypeError(msg)
	else:
		return dirname

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='Parse the Wikipedia sql dumps.')
	parser.add_argument('path', type=is_dir, nargs=1,
					   help='Path where the dumps are stored')
	parser.add_argument('type', type=str, nargs=1,
					   help=('File type: "redirect", "page", "pagelinks",' + 
							'"all" for all 3 files ' + 
							'or "combine" to combine them to remove redirects. '))

	args = parser.parse_args()
	print('Processing', args.path)
	path = args.path[0]
	file_type = args.type[0]
	if file_type == "all" or file_type == "combine":
		file_type_list = ["redirect.sql.gz", "page.sql.gz", "pagelinks.sql.gz"]
	else:
		file_type_list = [file_type + ".sql.gz"]
	file_list = glob.glob(os.path.join(path,'*'))
	#print(file_list)
	input_file_list = []
	for f_type in file_type_list:
		input_file_list += [file for file in file_list if f_type in file]
	if not input_file_list:
		print('No file found.')
		raise ValueError('No file to load in the given folder.')

	if file_type != "combine": # Just extract the files
		print('List to process:', input_file_list)		
		for input_file in input_file_list:
			process_file(input_file)
	else:
		pagelinks_corrected_file = 'pagelinks_corrected'
		# Load redirect info
		redirect_parsed = [file for file in file_list if 'redirect_parsed' in file]
		if redirect_parsed:
			if len(redirect_parsed) > 1:
				raise ValueError('Can not handle more than one parsed redirect file.',redirect_parsed)
			print('Found file already parsed. Loading {} ...'.format(redirect_parsed[0]))
			df_redirect = pandas.read_pickle(redirect_parsed[0])
		else:
			redirect_file = [file for file in input_file_list if 'redirect' in file]
			df_redirect = process_file(redirect_file[0])
		# load pageid info
		pageid_parsed = [file for file in file_list if 'pageid_parsed' in file]
		if pageid_parsed:
			if len(pageid_parsed) > 1:
				raise ValueError('Can not handle more than one parsed page file.',pageid_parsed)
			print('Found file already parsed. Loading {} ...'.format(pageid_parsed[0]))
			df_pageid = pandas.read_pickle(pageid_parsed[0])
		else:
			page_file = [file for file in input_file_list if 'page.' in file]
			print(input_file_list)
			df_pageid = process_file(page_file[0])
		# Load pagelinks info
		pagelinks_parsed =  [file for file in file_list if 'pagelinks_parsed' in file]
		df_redirect,df_pageid = df_reshape(df_redirect,df_pageid)
		if pagelinks_parsed:
			print('Found files already parsed:'.format(pagelinks_parsed))
			for file_nb,pagelinks_file in enumerate(sorted(pagelinks_parsed)):
				print('Loading',pagelinks_file)
				df_pagelinks = pandas.read_pickle(pagelinks_file)
				out_combine = os.path.join(path, pagelinks_corrected_file + str(file_nb) + '.npz')
				df_pagelinks = combine_info(df_redirect,df_pageid,df_pagelinks,out_combine)
		else:
			pagelinks_file = [file for file in input_file_list if 'pagelinks' in file]
			process_file(pagelinks_file[0],combine=[df_redirect,df_pageid,pagelinks_corrected_file])
			


#	input_file = '/home/benjamin/wikipedia/Wikipedia/enwiki-20180801-pagelinks.sql.gz'
#	input_file = '/home/benjamin/wikipedia/Wikipedia/enwiki-20180801-redirect.sql.gz'
#	input_file = '/home/benjamin/wikipedia/Wikipedia/enwiki-20180801-page.sql.gz'
#	folder,filename = os.path.split(input_file)

