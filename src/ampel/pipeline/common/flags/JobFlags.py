from enum import Flag

class JobFlags(Flag):
	"""
		Flags used by DBJobReporter when creating a document to be pushed to the collection "events".
		Since the documents, once created, are never updated (and thus the $bit operator 
		is not required), this class can embbed more than 64 different flags.
		HAS_ERROR and HAS_CRITICAL flags will be converted into a sparse indexed field named "err".
	"""
	NO_FLAG				= 0

	HAS_ERROR    		= 1 
	HAS_CRITICAL   		= 2

	T0		            = 4 
	T1		            = 8 
	T2		            = 16
	T3		            = 32 

	INST_ZTF			= 64
	INST_OTHER1			= 128
	INST_OTHER2			= 256
	INST_OTHER3			= 512

	PP_IPAC				= 1024
	PP_WZM				= 2048
	PP_HU				= 4096

	ALERT_IPAC			= 8192
	ALERT_OTHER			= 16384
	ALERT_OTHER2		= 32768

	NO_CHANNEL          = 65536 
	CHANNEL_SN          = 131072 
	CHANNEL_NEUTRINO    = 262144
	CHANNEL_RANDOM      = 524288
	CHANNEL_OTHER1      = 1048576
	CHANNEL_OTHER2      = 2097152
	CHANNEL_OTHER3      = 4194304

	T2_SNCOSMO			= 8388608
	T2_SNII_LC			= 16777216
	T2_AGN				= 33554432
	T2_PHOTO_Z			= 67108864
	T2_PHOTO_TYPE		= 134217728
	T2_OTHER1			= 268435456
	T2_OTHER2			= 536870912
	T2_OTHER3			= 1073741824
	T2_OTHER4			= 2147483648
	T2_OTHER5			= 4294967296

	T3_PURGE			= 8589934592
	T3_MARSHALL_PUSH	= 17179869184
	T3_JUPYTER			= 34359738368
	T3_RANKING			= 68719476736
	T3_ERROR_REPORTER	= 137438953472
