Indexing notes

data:
	'stock': 
	't0': 
	't1': 
	't2': 

var:
	'logs': 
		Note regarding the indexing of tranId: this is more of a convenience index. Transient docs contain a list of runIds which could greatly reduce the number of log entries to scan. Meaning matching tranId indirectly using runIds should be fairly quick. On the other hand, there should not be a lot of log entries associated with a tranId, so that the indexing perf penalty should not be an issue. Time will tell...
	'events': 
	'beacon': 
	'troubles': 

ext:
	'counter': 
	'journal': 
