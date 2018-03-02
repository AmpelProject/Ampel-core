
from sqlalchemy import Table, MetaData, Column, ForeignKey, ForeignKeyConstraint
from sqlalchemy import String, Integer, BigInteger, Float
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy import select, and_
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, OperationalError
import os, json
import fastavro

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

@compiles(Insert, 'postgresql')
def ignore_duplicates(insert, compiler, **kw):
    s = compiler.visit_insert(insert, **kw)
    ignore = insert.kwargs.get('postgresql_ignore_duplicates', False)
    return s if not ignore else s + ' ON CONFLICT DO NOTHING'
Insert.argument_for('postgresql', 'ignore_duplicates', None)

@compiles(Insert, 'sqlite')
def ignore_duplicates(insert, compiler, **kw):
    ignore = insert.kwargs.get('sqlite_ignore_duplicates', False)
    my_insert = insert if not ignore else insert.prefix_with('OR IGNORE')
    return compiler.visit_insert(my_insert, **kw)
Insert.argument_for('sqlite', 'ignore_duplicates', None)

class ArchiveDB(object):
    def __init__(self, *args, **kwargs):
        """
        Initialize and connect to archive database. Arguments will be passed on
        to :py:func:`sqlalchemy.create_engine`.
        """
        with open(os.path.dirname(os.path.realpath(__file__)) + '/../../../alerts/schema.json') as f:
            schema = json.load(f)
        self._meta = create_metadata(schema)
        engine = create_engine(*args, **kwargs)
        self._connection = engine.connect()
    
    def insert_alert(self, alert, processor_id, ingestion_time):
        insert_alert(self._connection, self._meta, alert, processor_id, ingestion_time)
    
    def get_alert(self, alert_id):
        return get_alert(self._connection, self._meta, alert_id)

def create_metadata(alert_schema):
    """
    Create archive database schema from AVRO alert schema
    
    :param alert_schema: the alert format, from e.g. avro.reader(file).schema
    :returns: an initialized :py:class:`sqlalchemy.MetaData`
    """
    
    fields = {f['name'] : f for f in alert_schema['fields']}

    double = Float(53).with_variant(DOUBLE_PRECISION(), "postgresql")
    types = {'float': Float(), 'double': double, 'int': Integer(), 'long': BigInteger(), 'string': String(255)}
    def make_column(field):
        kwargs = {}
        if type(field['type']) is list:
            typename = field['type'][0]
            if field['type'][1] == 'null':
                kwargs['nullable'] = True
        else:
            typename = field['type']
        if typename in types:
            type_ = types[typename]
        else:
            raise ValueError("Unknown field type '{}'".format(typename))
        
        return Column(field['name'], type_, **kwargs)

    meta = MetaData()

    Alert = Table('alert', meta,
        Column('candid', BigInteger(), primary_key=True, nullable=False),
        Column('objectId', String(12), nullable=False),
        Column('processor_id', Integer(), nullable=False),
        Column('ingestion_time', BigInteger(), nullable=False)
    )

    indices = {'candid', 'pid'}
    columns = filter(lambda c: c.name not in indices, map(make_column, fields['candidate']['type']['fields']))
    PhotoPoint = Table('candidate', meta,
        Column('candid', BigInteger(), primary_key=True, nullable=False),
        Column('pid', BigInteger(), primary_key=True, nullable=False),
        
        *columns
    )
    
    indices = {'candid', 'pid'}
    columns = filter(lambda c: c.name not in indices, map(make_column, fields['prv_candidates']['type'][0]['items']['fields']))
    PhotoPoint = Table('prv_candidate', meta,
        Column('candid', BigInteger(), primary_key=True, nullable=False),
        Column('pid', BigInteger(), primary_key=True, nullable=False),
        
        *columns
    )

    Pivot = Table('alert_prv_candidate_pivot', meta,
        Column('alert_id', BigInteger(), ForeignKey("alert.candid"), primary_key=True, nullable=False),
        Column('candid', BigInteger(), primary_key=True, nullable=False),
        Column('pid', BigInteger(), primary_key=True, nullable=False),
        Column('index', Integer(), nullable=False),
        ForeignKeyConstraint(['candid', 'pid'], ['prv_candidate.candid', 'prv_candidate.pid']),
    )

    return meta

def create_database(metadata, *args, **kwargs):
    """
    Initialize the archive database. Extra args and kwargs will be passed to
    :py:func:`sqlalchemy.create_database`.
    
    :param metadata: 
    """
    from sqlalchemy import create_engine
    
    engine = create_engine(*args, **kwargs)
    metadata.create_all(engine)
    
    return engine

def insert_alert(connection, meta, alert, processor_id, ingestion_time):
    """
    Insert an alert into the archive database
    
    :param connection: database connection
    :param meta: schema metadata
    :param alert: alert dict
    :param processor_id: the index of the :py:class:`ampel.t0.AlertProcessor`
                         that received the alert
    :param ingestion_time: time the alert was received, in UNIX epoch microseconds
    
    """
    alert_id = alert['candid']
    try:
        connection.execute(meta.tables['alert'].insert(),
            candid=alert_id, objectId=alert['objectId'],
            processor_id=processor_id, ingestion_time=ingestion_time)
        connection.execute(meta.tables['candidate'].insert(), **alert['candidate'])
    except IntegrityError:
        # abort on duplicate alerts
        return
    
    ignore = {'{}_ignore_duplicates'.format(connection.dialect.name): True}
    
    if len(alert['prv_candidates']) > 0:
        # entries in prv_candidates will often be duplicated, but may also
        # be updated without warning.
        connection.execute(meta.tables['prv_candidate'].insert(**ignore), alert['prv_candidates'])
        pivots = [dict(alert_id=alert_id, index=index, candid=v['candid'], pid=v['pid']) for index, v in enumerate(alert['prv_candidates'])]
        connection.execute(meta.tables['alert_prv_candidate_pivot'].insert(), pivots)
    return

def get_alert(connection, meta, alert_id):
    """
    Retrieve an alert from the archive database
    
    :param connection: database connection
    :param meta: schema metadata
    :param alert_id: `candid` of the alert to retrieve
    :returns: the target alert as a :py:class:`dict`, or `None` if the alert is
              not in the archive
    """
    alert = meta.tables['alert']
    result = connection.execute(select([alert.c.candid, alert.c.objectId]).where(meta.tables['alert'].c.candid == alert_id)).first()
    if result is None:
        return
    alert = dict(result)
    
    candidate = meta.tables['candidate']
    result = connection.execute(candidate.select().where(candidate.c.candid == alert['candid'])).first()
    alert['candidate'] = dict(result)
    
    alert['prv_candidates'] = []
    prv_candidate = meta.tables['prv_candidate']
    pivot = meta.tables['alert_prv_candidate_pivot']
    for result in connection.execute(select([prv_candidate]) \
         .select_from(prv_candidate.join(pivot)) \
         .where(pivot.c.alert_id == alert_id) \
         .order_by(pivot.c.index)).fetchall():
        alert['prv_candidates'].append(dict(result))
    
    return alert

def init_db():
	"""
	Initialize archive db for use with Ampel
	"""
	import os, time
	
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument('--host', default='localhost:5432',
	    help='Postgres server address and port')
	parser.add_argument('-d', '--database', default='ztfarchive',
	    help='Database name')
	parser.add_argument('--schema', default=os.path.dirname(os.path.realpath(__file__)) + '/../../../alerts/schema.json',
	    help='Alert schema in json format')
	
	opts = parser.parse_args()
	
	def env(var):
		if '{}_FILE'.format(var) in os.environ:
			with open(os.environ['{}_FILE'.format(var)]) as f:
				return f.read().strip()
		else:
			return os.environ[var]
	
	user = 'ampel'
	password = env('POSTGRES_PASSWORD')
	for attempt in range(10):
		try:
			engine = create_engine('postgresql://{}:{}@{}/{}'.format(user, password, opts.host, opts.database))
			break
		except OperationalError:
			if attempt == 9:
				raise
			else:
				time.sleep(1)
				continue
	
	with open(opts.schema) as f:
		schema = json.load(f)
	
	meta = create_metadata(schema)
	meta.create_all(engine)
	
