#!/usr/local/bin/python3

from __future__ import print_function
import sys, glob, os, time, getopt
import numpy as np
 
from astropy.io import fits as pyfits

# Ampel imports
#from ampel.pipeline.t0.AmpelAlert import AmpelAlert
#from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
from extcats import CatalogQuery 

class CrossCats():
	def __init__(self):

		# setup catalogs, this requires mongo db is running
		self.varstar_query = CatalogQuery.CatalogQuery("varstars", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')
		self.milliquas_query = CatalogQuery.CatalogQuery("milliquas", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')
		self.qsovar_query = CatalogQuery.CatalogQuery("qsoVarPTF", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')
		self.wise_query = CatalogQuery.CatalogQuery("wise_color", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')
		self.port_query = CatalogQuery.CatalogQuery("portsmouth", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')

	def get_info(self, ra, dec):
		'''
		>>info, z = get_info(ra, dec)
		
		run cross matching, collect some info 
		
		inpput ra, dec (deg), float

		returns: 
			- list with info per catalog match, or [] if no match
			- spectro redshift, None if no match
		'''

		info = []
		z = None
		classification = None

		is_varstar = self.varstar_query.binaryserach(ra,dec, 2., method='2dsphere')
		milliquas_match = self.milliquas_query.binaryserach(ra,dec, 2.)
		varstar_match = self.varstar_query.binaryserach(ra,dec, 2., method='2dsphere')
		wise_match = self.wise_query.binaryserach(ra, dec, 2.)
		qsovar_match = self.qsovar_query.binaryserach(ra,dec, 1.)
		port_match = self.port_query.binaryserach(ra,dec, 1.)


		if port_match:
			print ('portsmouth match')
			port_table,_ = self.port_query.findclosest(ra, dec, 2)
			if port_table is not None:
				info.append('From Portsmouth: sigma={0:0.1f}+/-{1:0.1f};  BPT={2}'.format(port_table['sigma_stars'], 
																								port_table['sigma_stars_err'],
																								port_table['bpt']))
				print (info[-1])
				z = port_table['z']
				if port_table['bpt']=='Seyfert':
					if classification is None:
						classification = None # because this doesn't work
				if port_table['bpt']=='LINER':
					if classification is None:
						classification = 'LINER'


		if milliquas_match:
			print ('milliquas match')
			milliquas_table,_ = self.milliquas_query.findclosest(ra, dec, 2)
			if milliquas_table is not None:
				ss = 'Milliquas match: broadtype={0}; ref={1}'.format(milliquas_table['broad_type'], milliquas_table['ref_name'])
				
				# check if this a photometric classification
				if 'q' in milliquas_table['ref_name']:
					ss.append('; photo_z={0:0.1}; QSO prob={1:0.1f}%'.format(milliquas_table['z'], milliquas_table['qso_prob']))
					if classification is None:
						classification = 'photo AGN'
				
				# only use spectrosopic redshifts for annotation
				elif (milliquas_table['ref_name'] =='X') or (milliquas_table['ref_name'] =='X') or (milliquas_table['ref_name'] =='RX'):
					dummy = '' 

				else:
					if classification is None:
						classification= 'AGN'
					if z is None:
						z = milliquas_table['redshift']

				print (ss)
				info.append(ss)
				


		if wise_match:
			print ('wise match')
			wise_table,_ = self.wise_query.findclosest(ra, dec, 2)
			if wise_table is not None:
				ss = 'WISE photo AGN: W1-W2={0:0.2}'.format(wise_table['W1_min_W2'])
				print (ss)
				info.append(ss)	
				classification = 'AGN'

		if qsovar_match:
			print ('qsovar match')
			qsovar_table,_ = self.qsovar_query.findclosest(ra, dec, 2)
			if qsovar_table is not None:
				ss='variablity selected QSO; var_chi2={0:0.1}'.format(qsovar_table['var_chi2'])
				info.append(ss)
				classification = 'AGN'

		if varstar_match:
			print ('varstar match')
			info.append('known variable star')
			classification = 'varstar'

		return info, classification, z

# allow a single call from the command line
def main(argv):
	
	if (len(sys.argv)) !=3:
		print ('syntax: crosscats.py ra dec')
		sys.exit(2)
	ra, dec = float(sys.argv[1]), float(sys.argv[2])

	cc = CrossCats()
	print ('')
	info, classification, z = cc.get_info(ra, dec)
	if len(info)==0:
		print ('no matches')



if __name__ == "__main__":
   main(sys.argv[1:])

# test with example AGN: ZTF18aacckko, ra, dec = 211.434899  40.854632
# ra, dec = 211.434899,  40.854632
# cm = CrossMatch()
# info, z = cm.get_info(ra, dec)
# print ('')
# print (info)




