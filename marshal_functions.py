#!/usr/local/bin/python
from __future__ import print_function

import urllib, urllib2,base64,pickle
from bs4 import BeautifulSoup
import re, os, datetime, json
import yaml
import astropy.time

marshal_root = 'http://skipper.caltech.edu:8080/cgi-bin/growth/'
listprog_url = marshal_root + 'list_programs.cgi'
scanning_url = marshal_root + 'growth_treasures_transient.cgi'
saving_url = marshal_root + 'save_cand_growth.cgi?candid=%s&program=%s'
rawsaved_url = marshal_root + 'list_sources_bare.cgi'
annotate_url = marshal_root + 'edit_comment.cgi'

class PTFConfig(object) :
	def __init__(self) :
		import ConfigParser
		self.fname = os.path.expanduser('~/.ptfconfig.cfg')
		self.config = ConfigParser.SafeConfigParser()
		self.config.read(self.fname)
	def get(self,*args,**kwargs) :
		return self.config.get(*args,**kwargs)

def get_marshal_html(weblink):
	request = urllib2.Request(weblink)
	conf = PTFConfig()
	base64string = base64.encodestring('%s:%s' % (conf.get('Marshal', 'user'), conf.get('Marshal', 'passw'))).replace('\n', '')
	request.add_header("Authorization", "Basic %s" % base64string)
	return urllib2.urlopen(request).read()

def soup_obj(url):
	return BeautifulSoup(get_marshal_html(url), 'lxml')

def save_source(candid, progid):
	return BeautifulSoup(get_marshal_html(saving_url %(candid, progid)), 'lxml') 



today = datetime.datetime.now().strftime('%Y-%m-%d')
fivedaysago = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y-%m-%d')

class Sergeant(object):
	'''
	>>> ser = Sergeant(program_name='ZTF Science Validation')

	optional input for constructor
	 start_date='2018-04-01'
	 end_date='2018-04-01'
	if none are given we assume today and five days ago
	'''
	
	def __init__(self, program_name='Nuclear Transients',start_date=None, end_date=None) :
		
		if start_date is None:
			start_date = fivedaysago
			print ('start_date : {0}'.format(start_date))
		if end_date is None:
			end_date = today
			print ('end_date   : {0}'.format(end_date))

		self.start_date = start_date
		self.end_date = end_date
		self.program_name = program_name
		self.cutprogramidx = None
		self.program_options =[]
 
		soup = soup_obj(listprog_url)

		for x in json.loads(soup.find('p').text.encode("ascii")):
			self.program_options.append(x['name'])
			if x['name'] == self.program_name:				
				self.cutprogramidx = x['programidx']
		
		if self.cutprogramidx is None:
			print ('ERROR, program_name={0} not found'.format(self.program_name))
			print ('Options for this user are:', self.program_options)
			return None
	
	def list_scan_sources(self, hardlimit=200):
		if self.cutprogramidx is None:
			print('ERROR, first fix program_name upon init')
			return []
		self.scan_soup = soup_obj(scanning_url + "?cutprogramidx=%s&startdate=%s&enddate=%s&HARDLIMIT=%s" %(self.cutprogramidx, self.start_date, self.end_date, hardlimit))

		
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

	def list_saved_sources(self):
		t_now = astropy.time.Time.now()
		if self.cutprogramidx is None:
			print('ERROR, first fix program_name upon init')
			return []
		self.saved_soup = soup_obj(rawsaved_url + "?programidx=%s" %(self.cutprogramidx))
		
		#print ('saved soup:',self.saved_soup)

		table = self.saved_soup.findAll('table')
		table_rows = table[1].findAll('tr')[1:-1]
		sources = []
		for row in table_rows:
			sources.append({})
			cells = row.findAll('td')
			if len(cells) > 1:
				try:
					sources[-1]["objname"] = cells[1].find('a').text
					sources[-1]["objtype"] = cells[2].find('font').text.strip()
					sources[-1]["z"] = cells[3].find('font').text.strip()
					sources[-1]["ra"], sources[-1]["dec"] = re.findall(r'<.*><.*>(.*?)<br/>(.*?)</font></td>', str(cells[4]))[0]
					sources[-1]["phot"], sources[-1]["dt"] = re.findall(r'<.*><.*>.*? = (.*?)<br/> \((.*?) d\)</font></td>', str(cells[5]))[0]
					sources[-1]["annotation"] = {}
					keys = cells[11].findAll('font', {'color': '#0072bc'})
					values = cells[-1].findAll('font', {'color': 'black'})
					for key_name, val in zip(keys, values):
						sources[-1]["annotation"][key_name.text.strip()] = val.text.strip()
					v = re.search(r'var data\d+\s*=(\s*(.*)\} \}\,\])', cells[6].find('script').text.replace('\n','')).group(1)
					plot_data = yaml.load(v)
					LC = {'detection': {}, 'upperlim': {}}
					for flot in plot_data:
						if flot['label'] != 'test' and flot['points']['show'] == True:
							if flot['label'] not in LC['detection']:
								LC['detection'][flot['label']] = []
								LC['upperlim'][flot['label']] = []

							if flot['points']['type'] == 'o':
								d = LC['detection']
							elif flot['points']['type'] == 'dV':
								d = LC['upperlim']
							for datapoints in flot['data']:
								if datapoints != []:
									# Plotted time is relative to the time of a db query when loading the script. 
									# There might be a systemic offset (~few seconds) from the actual MJD.
									d[flot['label']].append([ t_now.mjd + datapoints[0], -datapoints[1]])
					sources[-1]["LC"] = LC

				except IndexError:
					print('{0} has no annotation'.format(sources[-1]["objname"]))
		return sources

def get_comments(sourcename='',source={}):
	'''
	>>> some_str = get_comments('ZTF18aabtxvd')
	get the current comment for a source
	
	>>> some_str = get_comments(source=source)
	get the current comments, and add them the source dict 
	this dict is output from get_saved_sources

	'''

	if not('objname' in source) and not(sourcename):
		print('''ERROR, we need sourcename='ZTFxxxxxx' or source=dict with objname key''')
		return 

	if not sourcename:
		sourcename = source["objname"]	

	if source:
		source['comments'] = [] # we re-read the current comments

	soup = soup_obj(marshal_root + 'view_source.cgi?name=%s' %sourcename)
	table = soup.findAll('table')[0]
	cells = table.findAll('span')
	all_comments = [] 
	
	for cell in cells:
	
		cell_str = cell.decode(0)	
		if (cell_str.find('edit_comment')>0) or (cell_str.find('add_autoannotation')>0):
			lines = cell_str.split('\n')			
			if lines[5].find(':')>0:
				date_author, type = (lines[5].strip(']:').split('['))
				text = lines[9].strip()
				text = text.replace(', [',')') # this deals with missing urls to [reference] in auto_annoations
				one_line = '{0} [{1}]: {2}'.format(date_author, type, text)
				print (one_line)

				# add comments to source dict
				if source:
					source['comments'].append((date_author.strip(), type, text))
				all_comments.append( one_line )
				print ('---')
	return all_comments

def add_comment(comment, sourcename='',source={}, comment_type="info", force_classification=False):
	
	'''
	>>> soup_out = add_comment("dummy", sourcename='ZTF17aacscou')

	attempt is made to avoid duplicates
	when source dict is given as input we use this to check for current comments

	default comment type is "info", other options are "redshift", "classification", "comment"

	when pushing comment_type="classification", you have the option to 
	use force_classification=True to force overwrite of the  excisiting classification
	'''

	if not('objname' in source) and not(sourcename):
		print('''ERROR, we need sourcename='ZTFxxxxxx' or source=dict with objname key''')
		return 

	if not(sourcename):
		sourcename = source["objname"]	

	# check if already have a dict with current comments 
	if 'comments' in source:
		current_comm = [tup[2] for tup in source['comments']]
	else:
		print ('getting current comments...')
		current_comm = get_comments(sourcename=sourcename, source=source)

	#print (current_comm)
	if comment_type != 'classification':
		if comment in ''.join(current_comm):	
			print ('this comment was already made in current comments')
			return

	# extra strict checks to change the classification of a source
	else:
		if not force_classification:
			if not ('objtype' in source):
				print ('require source dict as input when adding classification \n(use force_classification=True to ignore this check)')
				return
			if source['objtype']!='None':
				print ('this source already has a classification:',source['objtype'])
				print ('(use force_classification=True to overwrite it)')
				return

	print ('setting up comment script...')
	soup = soup_obj(marshal_root + 'view_source.cgi?name=%s' %sourcename)
	cmd = {}
	for x in soup.find('form', {'action':"edit_comment.cgi"}).findAll('input'):
		if x["type"] == "hidden":
			cmd[x['name']] =x['value']
	cmd["comment"] = comment
	cmd["type"] = comment_type

	print ('pushing comment to marshal...')
	params = urllib.urlencode(cmd)
	try:
		return soup_obj(marshal_root + 'edit_comment.cgi?%s' %params)
	except error:
		#print (error)
		print ('timed out... trying one more time...')
		return soup_obj(marshal_root + 'edit_comment.cgi?%s' %params)


info_date = 'April 2018'
def get_some_info():
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
	SN II
	SN IIP
	SN IIL
	SN IIb
	SN IIn
	SN?
	SLSN-I
	SLSN-II
	SLSN-R
	SN I-faint
	Afterglow 
	QSO
	QSO?
	photo QSO
	AGN 
	AGN?
	photo AGN
	Seyfert 2
	Seyfert 1.9
	Seyfert 1.8
	Blazar
	LINER
	CLAGN
	CLQ
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

	progn = 'ZTF Science Validation'
	progn = 'Nuclear Transients'
	inst = Sergeant(progn)	
		
	#scan_sources = inst.list_scan_sources()
	#print ('# of scan sources', len(scan_sources) )
	saved_sources = inst.list_saved_sources()

	this_source = (item for item in saved_sources if item["objname"] == "ZTF18aagteoy").next()
	get_comments(this_source)

	#print (saved_sources)
	print ('# saved sources:',len(saved_sources)) 


if __name__ == "__main__":
	testing()


