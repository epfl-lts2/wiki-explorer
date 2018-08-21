
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

def extract_data_sliced(file,outname,regexparser,reduce2ns,ns,slice_size=1000):
	print('Extracting data from ',file)
	folder,filename = os.path.split(input_file)
	start_total = time.time()
	nb_outfile = 0
	total_nb_entries = 0
	for data_slice,line_count in data_slice_generator(file,regexparser,reduce2ns,ns,slice_size):
		outfile = os.path.join(folder,outname + str(nb_outfile) + '.gz')
		nb_entries = len(data_slice)
		if nb_entries > 0 :
			print('Nb of entries:',nb_entries)
			save_to_disk(data_slice,outfile)
			nb_outfile +=1
		total_nb_entries += nb_entries
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
		start_load = time.time()
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
				load_duration = time.time() - start_load
				print('\n Loading time: {} min {} s.'.format(int(load_duration/60),
					int(load_duration%60)))
				if not pageid_list :
					continue
				df=pandas.DataFrame(pageid_list,columns=['pageId','pageTitle'])
				df.pageId = df.pageId.astype(int)
				pageid_list = []
				start_load = time.time()
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
	return 0


def process_file(input_file):
	folder,filename = os.path.split(input_file)
	if 'pagelinks' in filename:
		print('Extracting links')
		regex_string = pagelinksR
		data_filter = reduce2ns_pagelinks
		outname = 'pagelinks_parsed'
		slice_size = 1000
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


	output_df = extract_data_sliced(input_file,outname,regex_string,data_filter,'0',slice_size)
	return output_df

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
	for f_type in file_type_list:
		input_file_list = [file for file in file_list if f_type in file]

	if not input_file_list:
		print('No file found.')
		raise ValueError('No file to load in the given folder.')

	if file_type != "combine": # Just extract the files
		print('List to process:', input_file_list)		
		for input_file in input_file_list:
			process_file(input_file)
	else:
		# Load redirect info
		redirect_parsed = [file for file in file_list if 'redirect_parsed' in file]
		if redirect_parsed:
			df_redirect = pandas.read_pickle(redirect_parsed[0])
		else:
			redirect_file = [file for file in input_file_list if 'redirect' in file]
			df_redirect = process_file(redirect_file[0])
		# load pageid info
		pageid_parsed = [file for file in file_list if 'page_parsed' in file]
		if pageid_parsed:
			df_pageid = pandas.read_pickle(pageid_parsed[0])
		else:
			page_file = [file for file in input_file_list if 'page.' in file]
			df_pageid = process_file(page_file[0])
		# Load pagelinks info
		pagelinks_parsed =  [file for file in file_list if 'pagelinks_parsed' in file]
		if not pagelinks_parsed:
			pagelink_file = [file for file in input_file_list if 'pagelinks' in file]
			df_pageid = process_file(pagelink_file[0])
		pagelinks_file = [file for file in input_file_list if 'pagelinks' in file]
		combine_files(df_redirect,df_pageid,pagelinks_file[0])



#	input_file = '/home/benjamin/wikipedia/Wikipedia/enwiki-20180801-pagelinks.sql.gz'
#	input_file = '/home/benjamin/wikipedia/Wikipedia/enwiki-20180801-redirect.sql.gz'
#	input_file = '/home/benjamin/wikipedia/Wikipedia/enwiki-20180801-page.sql.gz'
#	folder,filename = os.path.split(input_file)

