#!/usr/local/bin/python3

from __future__ import print_function
import sys, glob, os, time, getopt
import numpy as np
 
from astropy.io import fits as pyfits

# Ampel imports
#from ampel.pipeline.t0.AmpelAlert import AmpelAlert
#from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
from extcats import CatalogQuery 


def cc(ra, dec):
	'''
	>>info, z = cc(ra, dec)
	
	run cross matching, collect some info 
	
	inpput ra, dec (deg), float

	returns: 
		- list with info per catalog match, or [] if no match
		- spectro redshift, None if no match
	'''

	# setup catalogs
	varstar_query = CatalogQuery.CatalogQuery("varstars", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')
	milliquas_query = CatalogQuery.CatalogQuery("milliquas", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')
	qsovar_query = CatalogQuery.CatalogQuery("qsoVarPTF", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')
	wise_query = CatalogQuery.CatalogQuery("wise_color", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')
	port_query = CatalogQuery.CatalogQuery("portsmouth", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')
	#gaia_bright_query = CatalogQuery.CatalogQuery("gaia_dr1_m13", ra_key = 'ra', dec_key = 'dec', coll_name='srcs')

	is_varstar = varstar_query.binaryserach(ra,dec, 2., method='2dsphere')
	milliquas_match = milliquas_query.binaryserach(ra,dec, 2.)
	varstar_match = varstar_query.binaryserach(ra,dec, 2., method='2dsphere')
	qsovar_match = qsovar_query.binaryserach(ra,dec, 1.)
	port_match = port_query.binaryserach(ra,dec, 1.)
	wise_match = wise_query.binaryserach(ra, dec, 2.)
	#gaia_bright_search_rad = 5 # arcsec
	#gaia_bright_match = gaia_bright_query.binaryserach(ra,dec, gaia_bright_search_rad)

	info = []
	z = None

	if port_match:
		print ('portsmouth match')
		port_table,_ = port_query.findclosest(ra, dec, 2)
		if port_table is not None:
			info.append('SDSS spectro gal: sigma={0:0.1f}+/-{1:0.1f};  BPT={2}\n\n'.format(port_table['sigma_stars'], 
																							port_table['sigma_stars_err'],
																							port_table['bpt']))
			print (info[-1])
			z = port_table['z']

	if milliquas_match:
		print ('milliquas match')
		milliquas_table,_ = milliquas_query.findclosest(ra, dec, 2)
		if milliquas_table is not None:
			ss = 'milliquas match: broadtype={0}; ref={1}'.format(milliquas_table['broad_type'], 
																				milliquas_table['ref_name'])
			if 'q' in milliquas_table['ref_name']:
				ss.append('; photo_z={0:0.1}; QSO prob={1:0.1f}%'.format(milliquas_table['z'], milliquas_table['qso_prob']))
			
			# only use spectrosopic redshifts for annotation
			else:
				if z is not None:
					z = milliquas_table['redshift']

			print (ss)
			info.append(ss)

	if wise_match:
		print ('wise match')
		wise_table,_ = wise_query.findclosest(ra, dec, 2)
		if wise_table is not None:
			ss = 'WISE photo AGN: W1-W2={0:0.2}'.format(wise_table['W1-W2'])
			print (ss)
			info.append(ss)	

	if qsovar_match:
		print ('qsovar match')
		qsovar_table,_ = qsovar_match.findclosest(ra, dec, 2)
		if qsovar_table is not None:
			ss='variablity selected QSO; var_chi2={0:0.1}'.format(qsovar_table['var_chi2'])
			info.append(ss)

	if varstar_match:
		print ('varstar match')
		info.append('known variable star')

	return info, z



def main(argv):
	
	if (len(sys.argv)) !=3:
		print ('syntax: run_cc.py ra dec')
		sys.exit(2)
	ra, dec = float(sys.argv[1]), float(sys.argv[2])
	info, z = cc(ra, dec)



if __name__ == "__main__":
   main(sys.argv[1:])

# test with example AGN: ZTF18aacckko, ra, dec = 211.434899  40.854632
# ra, dec = 211.434899,  40.854632
# info, z = cc(ra, dec)
# print ('')
# print (info)




