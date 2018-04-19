'''
this script should keep listening to the Marshal 
to either save or add annotations
'''
import marshal_functions
from crosscats import CrossCats

from astropy import units as u
from astropy.coordinates import Angle

# sigh...
try:
   input = raw_input
except NameError:
   pass

safe_mode = False # ask for every addidtion

# setup classes
ser = marshal_functions.Sergeant('Nuclear Transients')
cc = CrossCats()


# here we need some smart loop that check every few hours for new sources
# ....

print ('reading saved sources....')
saved_sources = ser.list_saved_sources()
print ('# of sources', len(saved_sources))

for source in saved_sources[0:-1]:
	
	print (source['objname'])
	ra = Angle(source['ra'], unit='hour').degree
	dec = Angle(source['dec'], unit='deg').degree
	
	info_list, classification, z= cc.get_info(ra, dec)
	
	# get the current comments (and save them to source dict)
	if len(info_list)>0:
		
		print ('pulling current comments...')
		comment_list = marshal_functions.get_comments(source=source)		

		# if the comments list has an auto annotation with spectro redshift, 
		# and redshift not yet set, add comment with redshift
		comment_list_flat = ''.join(comment_list)
		for comment in comment_list:
			if ('[NED_redshift_auto]' in comment) or ('[SDSS_specz_auto]' in comment):
				try:
					z = float(comment.split(':')[1].split('+')[0].strip())
					print ('will try to add redshift z={0:0.4f} from comment:{1}', z, comment.split('[')[1].split()[']'][0])
					marshal_functions.add_comment(str(z), source=source, comment_type='redshift')		
				except IndexError, ValueError:
					z = None



	# loop over info obtained by running the cross matching
	for info in info_list:
		# check that comment is not already made	
		if not info in ''.join(comment_list):
			print ('will try to add info:', info)
			
			if safe_mode:
				key = input('y/[n]')
			else:
				key = 'y'
			
			if key=='y':
				marshal_functions.add_comment(info, source=source, comment_type='info')
	
	
	if (classification is not None) and (source['objtype']=='None'):
		
		print ('will try to add classification:', classification)	
		
		if safe_mode:
			key = input('y/[n]')
		else:
			key ='y'

		if key=='y':
			marshal_functions.add_comment(classification, source=source, comment_type='classification')



