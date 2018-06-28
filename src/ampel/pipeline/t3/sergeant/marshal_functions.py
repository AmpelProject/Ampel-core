#!/usr/bin/env python
# File              : pipeline/t3/marshal_functions.py
# License           : ?
# First author      : Tiara Hung <tiarahung@astro.umd.edu>  
# Second author 	: Sjoert van Velzen <sjoert@umd.edu>
# Date              : 12.04.2018
# Last Modified Date: 26.06.2018
# Last Modified By  : Sjoert 
from __future__ import print_function

import requests
import urllib, base64
import re, os, datetime, json, time
from collections import defaultdict

# less standard imports
import  configparser 			# pip3 install configparser
from bs4 import BeautifulSoup 	# pip3 install bs4 + pip3 install lxml
import yaml  					# pip3 install pyyaml
import astropy.time 			# pip3 install astropy

marshal_root = 'http://skipper.caltech.edu:8080/cgi-bin/growth/'
summary_url 	= marshal_root  + 'source_summary.cgi?sourceid=%s'
listprog_url = marshal_root + 'list_programs.cgi'
scanning_url = marshal_root + 'growth_treasures_transient.cgi'
saving_url = marshal_root   + 'save_cand_growth.cgi?candid=%s&program=%s'
savedsources_url = marshal_root + 'list_program_sources.cgi'
rawsaved_url = marshal_root + 'list_sources_bare.cgi'
annotate_url = marshal_root + 'edit_comment.cgi'
ingest_url = marshal_root + 'ingest_avro_id.cgi'




httpErrors = {
	304: 'Error 304: Not Modified: There was no new data to return.',
	400: 'Error 400: Bad Request: The request was invalid. An accompanying error message will explain why.',
	403: 'Error 403: Forbidden: The request is understood, but it has been refused. An accompanying error message will explain why',
	404: 'Error 404: Not Found: The URI requested is invalid or the resource requested, such as a category, does not exists.',
	500: 'Error 500: Internal Server Error: Something is broken.',
	503: 'Error 503: Service Unavailable.'
}





class PTFConfig(object) :
	def __init__(self) :		
		self.config_fname = os.path.expanduser('~/.ptfconfig.cfg')
		self.config = configparser.ConfigParser()
		self.config.read(self.config_fname)
	def get(self,*args,**kwargs) :
		return self.config.get(*args,**kwargs)

def get_marshal_html(weblink, attempts=1, max_attempts=5, marshalusr=None,marshalpwd=None):
			
	auth = requests.auth.HTTPBasicAuth(marshalusr, marshalpwd)
				
	try:
		reponse = requests.get(weblink, auth=auth, timeout=120+60*attempts)
	
	except Exception as e:

		print ('Sergeant.get_marshal_html(): ', weblink)
		print (e)
		print ('Sergeant.get_marshal_html(): this is our {0:0} attempt, {1:1} left'.format(attempts, max_attempts))

		if attempts<max_attempts:
			time.sleep(3)
			reponse.text = get_marshal_html(weblink, attempts=attempts+1, max_attempts=max_attempts)	
		else:
			print ('Sergeant.get_marshal_html(): giving up')
			raise(requests.exceptions.ConnectionError)

	return reponse.text



def post_marshal_cgi(weblink,data=None,attempts=1,max_attempts=5,marshalusr=None,marshalpwd=None):
	"""
	Run one of the growth cgi scripts, check results and return
	"""
	
	# option to overwrite default username from __init__
	if (marshalusr is None) or (marshalpwd is None):
		marshalusr, marshalpwd  = self.marshalusr, self.marshalpwd	
	
	auth = requests.auth.HTTPBasicAuth(marshalusr, marshalpwd)
	
	try:
		response = requests.post(weblink, auth=auth, data=data,timeout=120+60*attempts)
	except (requests.ConnectionError, requests.ReadTimeout) as req_e:		

		print ('Sergeant.post_marshal_cgi(): ', weblink)
		print (req_e)
		print ('Sergeant.post_marshal_cgi(): ConnectionError or ReadTimeout this is our {0} attempt, {1} left', attempts, max_attempts-max_attempts)

		if attempts<max_attempts:
			time.sleep(3)
			response = post_marshal_cgi(weblink, attempts=attempts+1, marshalusr=marshalusr,marshalpwd=marshalpwd)	
		else:
			print ('Sergeant.post_marshal_cgi(): giving up')
			raise(requests.exceptions.ConnectionError)

	return response
			
 


def json_obj(weblink,data=None,verbose=False, marshalusr=None,marshalpwd=None):
		'''
		Try to post to the marshal, then parse then return (assuming json)
		'''
		

		if verbose : print("Trying to post to marshal: "+weblink)
		
		r = post_marshal_cgi(weblink,data=data, marshalusr=marshalusr,marshalpwd=marshalpwd)
		print(r)
		status = r.status_code
		print(status)
		if status != 200:
				try:
						message = httpErrors[status]
				except KeyError:
						message = 'Error %d: Undocumented error' % status
						if verbose : print(message)
				return None
		if verbose : print("Successful growth connection")
	
		try:
				rinfo =  json.loads(r.text)
		except ValueError as e:
				# No json information returned, usually the status most relevant
				if verbose:
						print('No json returned: status %d' % status )
				rinfo =  status
		return rinfo

		

def soup_obj(url, marshalusr=None,marshalpwd=None):
	return BeautifulSoup(get_marshal_html(url, marshalusr=marshalusr,marshalpwd=marshalpwd), 'lxml')

def save_source(candid, progid, marshalusr=None,marshalpwd=None):
	return BeautifulSoup(get_marshal_html(saving_url %(candid, progid), marshalusr=marshalusr,marshalpwd=marshalpwd), 'lxml') 


class Sergeant(object):
	'''
	>>> ser = Sergeant(program_name='ZTF Science Validation')

	optional input for constructor (only used for the list_scan_sources function)
	 start_date='2018-04-01'
	 end_date='2018-04-01'
	if none are given we assume today and five days ago
	'''
	
	def __init__(self, program_name='Nuclear Transients',start_date=None, end_date=None,marshalusr=None,marshalpwd=None):
		
		today = datetime.datetime.now().strftime('%Y-%m-%d')
		fivedaysago = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y-%m-%d')

		if start_date is None:
			start_date = fivedaysago			
		if end_date is None:
			end_date = today
			
		self.start_date = start_date
		self.end_date = end_date
		self.program_name = program_name
		self.cutprogramidx = None
		self.program_options =[]
		
		# if no username given as input, look for "PTFconfig" file
		if (marshalusr is None) or (marshalpwd is None):
			conf = PTFConfig()			
			self.marshalusr, self.marshalpwd = conf.get('Marshal', 'user'), conf.get('Marshal', 'passw')
		else:
			self.marshalusr = marshalusr
			self.marshalpwd = marshalpwd
				
		soup = soup_obj(listprog_url,marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)

		for x in json.loads(soup.find('p').text.encode("ascii")):
			self.program_options.append(x['name'])
			if x['name'] == self.program_name:				
				self.cutprogramidx = x['programidx']
		
		if self.cutprogramidx is None:
			print ('ERROR, program_name={0} not found'.format(self.program_name))
			print ('Options for this user are:', self.program_options)
			return None


	def list_my_programids(self):
		print('My current programs:', self.program_options)


		def set_programid(self,programname):
				soup = soup_obj(listprog_url,marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)

		self.cutprogramidx = None
				
		for x in json.loads(soup.find('p').text.encode("ascii")):
			if x['name'] == programname:				
				self.cutprogramidx = x['programidx']
				self.program_name = programname
								
		if self.cutprogramidx is None:
			print ('ERROR, program_name={0} not found'.format(self.program_name))
			print ('Options for this user are:', self.program_options)
			return None
				
		return selt.cutprogramidx
						
				
	def list_scan_sources(self, hardlimit=200):
		print ('start_date : {0}'.format(self.start_date))
		print ('end_date   : {0}'.format(self.end_date))

		if self.cutprogramidx is None:
			print('ERROR, first fix program_name upon init')
			return []
		self.scan_soup = soup_obj(scanning_url + "?cutprogramidx=%s&startdate=%s&enddate=%s&HARDLIMIT=%s" %\
				(self.cutprogramidx, self.start_date, self.end_date, hardlimit),marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)

		
		table = self.scan_soup.findAll('table')
		table_rows = table[1].findAll('tr')[1:]
		
		# this fails if no sources are present on the scanning page...
		if len(table_rows)>0:
			for x in table_rows[0].findAll('td')[5].findAll('select')[0].findAll('option'):
				if self.program_name in x.text:
					self.program = x["value"]
		else:
			print ('No sources on scan page')
			self.program = None

		sources = []
		for source in table_rows:
			sources.append({})
			sources[-1]["candid"] = source.findAll('td')[5].findAll('input', {"name":'candid'})[0]["value"]
			for x in source.findAll('td')[5].findAll('b'):
				if x.text.strip() == 'ID:':
					sources[-1]["name"] = x.next_sibling.strip()
				elif x.text.strip() == 'Coordinate:':
					sources[-1]["ra"], sources[-1]["dec"] = x.next_sibling.split()
		
			for tag in table_rows[0].findAll('td')[-1].findAll('b'):
				key = tag.text.replace(u'\xa0', u'')
				sources[-1][key.strip(':')] = tag.next_sibling.strip()
		return sources


	def get_sourcelist(self):
		'''
		Return a list of sources saved to the program.
		Much faster than list_saved_sources, but no annotation or photometry information
		'''

		return  json_obj(savedsources_url,data={'programidx':str(self.cutprogramidx)}, marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)


	def ingest_avro_id(self, avroid):
		'''
		Ingest an alert from avro id.
		Todo: Update to bulk ingestion, check whether already saved or ingested?
		'''

		
		return json_obj(ingest_url,data={'programidx':str(self.cutprogramidx),'avroid':str(avroid)}, marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)

				
		
	def list_saved_sources(self, lims=False, maxpage=1e99, verbose=False):
		'''
		read all sources from the Saved Sources page(s)
		return a list of dictionaries with info (eg, coordinates, light curve)

		>>> ser = Sergeant()
		>>> sources = ser.list_saved_sources()

		the lims keyword can be used to turn on/off upper limits in the light curve

		'''
		t_now = astropy.time.Time.now()
		if self.cutprogramidx is None:
			print('ERROR, first fix program_name upon init')
			return []
		targ0 = 0
		page_number = 1
		sources = []
		while True:

			if page_number>maxpage:
				break

			if verbose:
				print ('list_saved_sources: reading page {0}'.format(page_number))				
								
			self.saved_soup = soup_obj(rawsaved_url + "?programidx=%s&offset=%s" %\
				(self.cutprogramidx, targ0),marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)

			table = self.saved_soup.findAll('table')
			table_rows = table[1].findAll('tr')[1:-1]
			if len(table_rows) < 2:
				break
			for row in table_rows:
				cells = row.findAll('td')
				if len(cells) > 1:
					sources.append({})
					try:
						sources[-1]["name"] = cells[1].find('a').text
						sources[-1]["objtype"] = cells[2].find('font').text.strip()
						sources[-1]["z"] = cells[3].find('font').text.strip()
						sources[-1]["ra"], sources[-1]["dec"] = re.findall(r'<.*><.*>(.*?)<br/>(.*?)</font></td>', str(cells[4]))[0]
						try:
							sources[-1]["phot"], sources[-1]["dt"] = re.findall(r'<.*><.*>.*? = (.*?)<br/> \((.*?) d\)</font></td>', str(cells[5]))[0]
						except IndexError:
							sources[-1]["phot"], sources[-1]["dt"] = re.findall(r'<.*><.*>.*? \&gt; (.*?)<br/> \((.*?) d\)</font></td>', str(cells[5]))[0]
							sources[-1]["phot"] = '>' + sources[-1]["phot"]
						sources[-1]["annotation"] = {}
						keys = cells[11].findAll('font', {'color': '#0072bc'})
						values = cells[-1].findAll('font', {'color': 'black'})
						for key_name, val in zip(keys, values):
							sources[-1]["annotation"][key_name.text.strip()] = val.text.strip()
						v = re.search(r'var data\d+\s*=(\s*(.*)\} \}\,\])', cells[6].find('script').text.replace('\n','')).group(1)
						plot_data = yaml.load(v)
						LC = {'detection': {}}
						for flot in plot_data:
							if flot['label'] != 'test' and flot['points']['show'] == True:
								if flot['label'] not in LC['detection']:
									LC['detection'][flot['label']] = []
									if lims == True:
										if 'upperlim' not in LC:
											LC['upperlim'] = {}
										LC['upperlim'][flot['label']] = []

								if flot['points']['type'] == 'o':
									d = LC['detection']
								elif flot['points']['type'] == 'dV' and lims == True:
									d = LC['upperlim']
								else:
									d = defaultdict(list)
								for datapoints in flot['data']:
									if datapoints != []:
										d[flot['label']].append([t_now.mjd + datapoints[0], -datapoints[1]])
						sources[-1]["LC"] = LC

					except IndexError:
						if verbose:
							print('{0} has no auto_annotation'.format(sources[-1]["name"]))
			targ0 += 100
			page_number+=1
		return sources



	def _parse_source_input(self, source):
		'''
		lil help with input (dict or ztfname)
		'''
		if type(source) is list:
			if len(source)==1:
				source = source[0]

		if "objname" in source: 		# not used anymore (keep for bc)
			sourcename = source['objname']
		elif "name" in source:
			sourcename = source['name'] # consistent with json key from list_program_sources.cgi
		elif type(source) is str:
			sourcename = source
		else:
			raise Exception('Source input not understood:',  source)
			return 


		return sourcename

	def get_summary(self, source):
		'''
		>> data = get_summary(source)

		call source_summary.cgi
		input source dict needs to have key "id"
		return json object with Marshal photometry and AUTO annotations, 
		to get the manual annotations, use Sergeant.get_annotations(source)
		'''

		if not("id" in source):
			raise Exception('''we need source dict with key "id "to use this source_summary.cgi''')

		soup = soup_obj(summary_url %source['id'],marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)

		try:
			phot_dict = json.loads(soup.find('p').text)
			return phot_dict
		
		except json.JSONDecodeError:
			print ('something wrong with this summary page:')
			print (summary_url%source['id'])
			return None


		

	def get_annotations(self, source, verbose=False):
		'''
		two inputs are possible:

		>>> comment_list = get_annotations('ZTF18aabtxvd')
		get the current comment for a source
		
		>>> comment_list = get_annotations(source_dict)
		get the current annotations, and add them the source dict 
		(this dict is output from list_saved_sources() or get_sourcelist() function)

		TODO: also get the info about scheduled observations
		'''

		sourcename = self._parse_source_input(source)

		if type(source) is dict:
			source["annotations"] = [] # replace, because we re-read the current annotations


		soup = soup_obj(marshal_root + 'view_source.cgi?name=%s' %\
			sourcename, marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)
		table = soup.findAll('table')[0]
		cells = table.findAll('span')
		
		all_annotations = []
		
		for cell in cells:
		
			cell_str = cell.decode(0)	
			if (cell_str.find('edit_comment')>0) or (cell_str.find('add_autoannotation')>0):
				lines = cell_str.split('\n')			
				if lines[5].find(':')>0:
					
					comment_id = int(lines[2].split('id=')[1].split('''"''')[0])					
					date_author, comment_type = (lines[5].strip(']:').strip().split('['))
					date, author = '-'.join(date_author.split(' ')[0:3]), date_author.split(' ')[3].strip()
					text = lines[9].strip()
					text = text.replace(', [',')') # this deals with missing urls to [reference] in auto_annoations
					one_line = '{0} [{1}]: {2}'.format(date_author, comment_type, text)
						
					# add new annotations to source dict
					comment_tuple = (comment_id, date, author, comment_type, text)
					if "annotations" in source:
						source["annotations"].append( comment_tuple )
					# add to the output dict
					all_annotations.append( comment_tuple )
					
					if verbose:
						print ('comment id =',comment_id)
						print (one_line)
						print ('---')
		return all_annotations

	def comment(self, comment, source, comment_type="info", comment_id=None, remove=False):
		
		'''
		two types of input are accepted:
		
		>>> soup_out = comment("dummy", 'ZTF17aacscou')
		>>> soup_out = comment("dummy", source_dict)

		here source_dict is a dictionary with keys "name" and (optional) "annotations"


		optional input:

		comment _type="info", other options are "redshift", "classification", "comment"	
		comment_id=123 to replace an excisiting comment, give its id as input 

		in this function an attempt is made to avoid duplicates
		when source_dict is given as input, we use this to check for current annotations
		otherwise we fill read the Marshal to get the current annotations.
		annotations are not added if identical text is found for this comment_type (by any user)

		removing comment is not yet implemented
		'''


		sourcename = self._parse_source_input(source)
				

		# check if already have a dict with current annotations
		# (we check only the comment text, not the Marshal username)
		if "annotations" in source:
			comment_list = source["annotations"]
		else:
			print ('getting current annotations...')
			comment_list = self.get_annotations(source)
		
		current_comm = ''.join([tup[4] for tup in comment_list if tup[3]==comment_type])

		
		if comment in ''.join(current_comm):	
			print ('this comment was already made in current annotations')
			current_id = [ tup[0] for tup in comment_list if ((tup[3]==comment_type) and (comment in tup[4]))][0] # perhaps too pythonic...?
			print ('to replace it, call this function with comment_id={}'.format(current_id))
			return current_id


		print ('setting up comment script for {0}...'.format(sourcename))
		soup = soup_obj(marshal_root + 'view_source.cgi?name=%s' %\
				sourcename,marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)
		cmd = {}
		for x in soup.find('form', {'action':"edit_comment.cgi"}).findAll('input'):
			if x["type"] == "hidden":
				cmd[x['name']] =x['value']
		cmd["comment"] = comment
		cmd["type"] = comment_type
		if comment_id is not None:
			cmd["id"] = str(comment_id)

		print ('pushing comment to marshal...')
		try:
			# to make this more elegant, the cmd could also be passed to request directly: request.get(marshal_root + 'edit_comment.cgi',cmd)
			params = urllib.parse.urlencode(cmd) # python3 
		except AttributeError:
			params = urllib.urlencode(cmd) # python2	
		
		return soup_obj(marshal_root + 'edit_comment.cgi?%s' %params,marshalusr=self.marshalusr,marshalpwd=self.marshalpwd)
	

info_date = 'April 2018'
def get_Marshal_info():
	program_names=\
	'''
	22 Cataclysmic Variables (PI = Paula Szkody)
	19 Census of the Local Universe (PI = David Cook)
	32 Cosmology (PI = Ulrich Feindt)
	17 Electromagnetic Counterparts to Gravitational Waves (PI = Mansi Kasliwal)
	25 Electromagnetic Counterparts to Neutrinos (PI = Anna Franckowiak)
	 5 Failed Supernovae (PI = Scott Adams)
	 9 Fast Transients (PI = Anna Ho)
	1 Fremling Subtractions (PI = Christoffer Fremling)
	12 Graham Nuclear Transients (PI = Matthew Graham)
	14 Infant Supernovae (PI = Avishay Gal-Yam)
	10 Nuclear Transients (PI = Suvi Gezari)
	31 Orphan Afterglows (PI = Anna Ho)
	24 Redshift Completeness Factor (PI = Shri Kulkarni)
	15 Red Transients (PI = Mansi Kasliwal)
	29 Stripped Envelope Supernovae (PI = jesper sollerman)
	 7 Superluminous Supernovae (PI = Lin Yan)
	27 Transients in Elliptical Galaxies (PI = Danny Goldstein)
	13 Variable AGN (PI = Matthew Graham)
	21 Young Stars (PI = lynne hillenbrand)
	3 TF Science Validation (PI = Christoffer Fremling)
	4 AMPEL Test (PI = Jakob Nordin)
	'''

	classifictions= \
	'''
	unknown
	SN
	SN Ia
	SN Ia 91bg-like
	SN Ia 91T-like
	SN Ia 02cx-like
	SN Ia 02ic-like
	SN Ia pec
	SN Ib/c
	SN Ib
	SN Ibn
	SN Ic
	SN Ic-BL
	SN II">SN II
	SN IIP">SN IIP
	SN IIL">SN IIL
	SN IIb">SN IIb
	SN IIn">SN IIn
	SN?
	SLSN-I
	SLSN-II
	SLSN-R
	SN I-faint
	Afterglow
	AGN
	AGN?
	CV
	CV?
	LBV
	galaxy
	varstar
	nova
	ILRN
	TDE
	Gap
	Gap I
	Gap II
	Gap I - Fast
	Gap I - Ca-rich
	Gap II - ILRT
	Gap II - LRN
	Gap II - LBV
	'''

	return program_names, classifictions


# testing
def testing():

	import numpy

	progn = 'ZTF Science Validation'
	#progn = 'Nuclear Transients'
	sergeant = Sergeant(progn)	


	print ('reading sourcelist for {0}...'.format(progn))
	sourcelist = sergeant.get_sourcelist()
	print ('# saved sources:',len(sourcelist)) 

	#this_source = (item for item in saved_sources if item["name"] == "ZTF18aagteoy").next()
	if len(sourcelist)>1:
		this_source  = sourcelist[int(numpy.random.rand()*len(sourcelist))] # pick one 
		print ('trying to get summary for ', this_source["name"], '...')
		summary = sergeant.get_summary(this_source)
		print ('autoannotations: ', summary["autoannotations"])
		print ('# of photometry points', len(summary["uploaded_photometry"]))
		print ('')
		print ('trying to get annotations for ', this_source["name"], '...')
		print ( sergeant.get_annotations(this_source) )

	print ('reading scan sources for {0}...'.format(progn))
	scan_sources = sergeant.list_scan_sources()
	print ('# of scan sources', len(scan_sources) )




if __name__ == "__main__":
	testing()


