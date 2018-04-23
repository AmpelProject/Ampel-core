
from sqlalchemy import Table, MetaData, Column, ForeignKey, ForeignKeyConstraint, Index
from sqlalchemy import String, Integer, BigInteger, Float
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy import select, and_, bindparam
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, OperationalError
import sqlalchemy
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
    
    def insert_alert(self, alert, partition_id, ingestion_time):
        """
        :param alert: alert dictionary
        :param partition_id: index of the Kafka partition this alert came from
        """
        insert_alert(self._connection, self._meta, alert, partition_id, ingestion_time)
    
    def get_alert(self, alert_id):
        return get_alert(self._connection, self._meta, alert_id)

    def get_alerts(self, alert_ids):
        return get_alerts(self._connection, self._meta, alert_ids)

    def get_alerts_in_time_range(self, jd_min, jd_max, partitions=None):
        return get_alerts_in_time_range(self._connection, self._meta, jd_min, jd_max, partitions)

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
        Column('partition_id', Integer(), nullable=False),
        Column('ingestion_time', BigInteger(), nullable=False),
        Column('jd', double, nullable=False),
        Index('alert_playback', 'partition_id', 'jd')
    )

    indices = {'candid', 'pid'}
    columns = filter(lambda c: c.name not in indices, map(make_column, fields['candidate']['type']['fields']))
    Table('candidate', meta,
        Column('candid', BigInteger(), ForeignKey("alert.candid"), primary_key=True, nullable=False),
        Column('pid', BigInteger(), primary_key=True, nullable=False),
        
        *columns
    )
    
    indices = {'candid', 'pid'}
    columns = filter(lambda c: c.name not in indices, map(make_column, fields['prv_candidates']['type'][0]['items']['fields']))
    Table('prv_candidate', meta,
        Column('candid', BigInteger(), primary_key=True, nullable=False),
        Column('pid', BigInteger(), primary_key=True, nullable=False),
        
        *columns
    )

    Table('alert_prv_candidate_pivot', meta,
        Column('alert_id', BigInteger(), ForeignKey("alert.candid"), primary_key=True, nullable=False),
        Column('candid', BigInteger(), primary_key=True, nullable=False),
        Column('pid', BigInteger(), primary_key=True, nullable=False),
        Column('index', Integer(), nullable=False),
        ForeignKeyConstraint(['candid', 'pid'], ['prv_candidate.candid', 'prv_candidate.pid']),
    )

    identifiers = ['jd', 'fid', 'pid', 'diffmaglim', 'pdiffimfilename']
    fluff = ['programpi', 'programid']
    columns = list(filter(lambda c: c.name in identifiers+fluff, map(make_column, fields['prv_candidates']['type'][0]['items']['fields'])))
    columns.append(Index('unique', *identifiers))
    Table('upper_limit', meta,
        Column('id', Integer(), primary_key=True, autoincrement=True),
        *columns
    )

    Table('alert_upper_limit_pivot', meta,
        Column('alert_id', BigInteger(), ForeignKey("alert.candid"), primary_key=True, nullable=False),
        Column('upper_limit_id', BigInteger(), ForeignKey("upper_limit.id"), primary_key=True, nullable=False),
        Column('index', Integer(), primary_key=True, nullable=False),
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

def insert_alert(connection, meta, alert, partition_id, ingestion_time):
    """
    Insert an alert into the archive database
    
    :param connection: database connection
    :param meta: schema metadata
    :param alert: alert dict
    :param partition_id: the index of the Kafka partition this alert came from
    :param ingestion_time: time the alert was received, in UNIX epoch microseconds
    
    """
    alert_id = alert['candid']
    try:
        connection.execute(meta.tables['alert'].insert(),
            candid=alert_id, objectId=alert['objectId'],
            partition_id=partition_id, ingestion_time=ingestion_time,
            jd=alert['candidate']['jd'])
        connection.execute(meta.tables['candidate'].insert(), **alert['candidate'])
    except IntegrityError:
        # abort on duplicate alerts
        return

    if alert['prv_candidates'] is None or len(alert['prv_candidates']) == 0:
        return

    # entries in prv_candidates will often be duplicated, but may also
    # be updated without warning. sort these into detections (which come with
    # unique ids) and upper limits (which don't)
    detections = dict(rows=[], pivots=[])
    upper_limits = dict(rows=[], pivots=[])
    for index, c in enumerate(alert['prv_candidates']):
        # entries with no candid are nondetections
        if c['candid'] is None:
            upper_limits['rows'].append(c)
            upper_limits['pivots'].append(dict(alert_id=alert_id, index=index))
        else:
            detections['rows'].append(c)
            detections['pivots'].append(dict(alert_id=alert_id, index=index, candid=c['candid'], pid=c['pid']))

    if len(detections['rows']) > 0:
        ignore = {'{}_ignore_duplicates'.format(connection.dialect.name): True}
        connection.execute(meta.tables['prv_candidate'].insert(**ignore), detections['rows'])
        connection.execute(meta.tables['alert_prv_candidate_pivot'].insert(), detections['pivots'])

    if len(upper_limits['rows']) > 0:
        UpperLimit = meta.tables['upper_limit']
        UpperLimitPivot = meta.tables['alert_upper_limit_pivot']
        index_cols = list(UpperLimit.indexes)[0].columns
        stmt = select([UpperLimit.c.id]).where(and_(*(c == bindparam(c.name) for c in index_cols)))
        for row, pivot in zip(upper_limits['rows'], upper_limits['pivots']):
            key = connection.execute(stmt, **row).fetchone()
            if key is None:
                key = connection.execute(UpperLimit.insert().values(**{k:row[k] for k in row if k in UpperLimit.columns})).inserted_primary_key
            pivot['upper_limit_id'] = key[0]
        connection.execute(UpperLimitPivot.insert(), upper_limits['pivots'])

def get_alert(connection, meta, alert_id):
    """
    Retrieve an alert from the archive database
    
    :param connection: database connection
    :param meta: schema metadata
    :param alert_id: `candid` of the alert to retrieve
    :returns: the target alert as a :py:class:`dict`, or `None` if the alert is
              not in the archive
    """
    Alert = meta.tables['alert']

    for alert in fetch_alerts_with_condition(connection, meta, Alert.c.candid == alert_id):
        return alert
    return None

def get_alerts(connection, meta, alert_ids):
    """
    Retrieve alerts from the archive database by ID
    
    :param connection: database connection
    :param meta: schema metadata
    :param alert_id: a collection of `candid` of alerts to retrieve
    :returns: the target alert as a :py:class:`dict`, or `None` if the alert is
              not in the archive
    """
    Alert = meta.tables['alert']
    # mimic mysql field() function, passing the order by hand
    order = sqlalchemy.text(','.join(('alert.candid=%d DESC' % i for i in alert_ids)))

    yield from fetch_alerts_with_condition(connection, meta, Alert.c.candid.in_(alert_ids), order)

def get_alerts_in_time_range(connection, meta, jd_start, jd_end, partitions=None):
    """
    Retrieve a range of alerts from the archive database

    :param connection: database connection
    :param meta: schema metadata
    :param jd_start: minimum JD of exposure start
    :param jd_end: maximum JD of exposure start
    :param partitions: range of partitions to consume. Clients with disjoint
        partitions will not receive duplicate alerts even if they request
        overlapping time ranges.
    :type partitions: int or slice
    """
    Alert = meta.tables['alert']
    in_range = and_(Alert.c.jd >= jd_start, Alert.c.jd < jd_end)
    if isinstance(partitions, int):
        in_range = and_(in_range, Alert.c.partition_id == partitions)
    elif isinstance(partitions, slice):
        assert partitions.step == 1 or partitions.step is None
        in_range = and_(in_range, and_(Alert.c.partition_id >= partitions.start, Alert.c.partition_id < partitions.stop))
    elif partitions is not None:
        raise TypeError("partitions must be int or slice")

    yield from fetch_alerts_with_condition(connection, meta, in_range, Alert.c.jd.asc())

def fetch_alerts_with_condition(connection, meta, condition, order=None):

    PrvCandidate = meta.tables['prv_candidate']
    UpperLimit = meta.tables['upper_limit']
    Candidate = meta.tables['candidate']
    Alert = meta.tables['alert']
    Pivot = meta.tables['alert_prv_candidate_pivot']
    UpperLimitPivot = meta.tables['alert_upper_limit_pivot']
    from sqlalchemy.sql import null

    alert_query = \
        select([Alert.c.objectId, Candidate])\
        .select_from(Alert.join(Candidate))\
        .where(condition).order_by(order)

    # build a query for detections
    cols = [Pivot.c.index,PrvCandidate]
    prv_query = \
        select(cols) \
        .select_from(PrvCandidate.join(Pivot)) \
        .where(Pivot.c.alert_id == bindparam('candid'))

    # and a corresponding one for upper limits, padding out missing columns
    # with null. Note that the order of the columns must be the same, for
    # the union query below to map the result correctly to output keys.
    cols = [UpperLimitPivot.c.index]
    for c in PrvCandidate.columns:
        if c.name in UpperLimit.columns:
            cols.append(UpperLimit.columns[c.name])
        else:
            cols.append(null().label(c.name))
    ul_query = \
        select(cols) \
        .select_from(UpperLimit.join(UpperLimitPivot)) \
        .where(UpperLimitPivot.c.alert_id == bindparam('candid'))

    # unify!
    history_query = prv_query.union(ul_query)

    for result in connection.execute(alert_query):
        candidate = dict(result)
        alert = dict(objectId=candidate.pop('objectId'), candid=candidate['candid'])
        alert['candidate'] = candidate
        alert['prv_candidates'] = []
        for result in connection.execute(history_query, candid=alert['candid']):
            alert['prv_candidates'].append(dict(result))

        alert['prv_candidates'] = sorted(alert['prv_candidates'], key=lambda c: c['index'])
        for c in alert['prv_candidates']:
            del c['index']

        yield alert

def docker_env(var):
	"""
	Read var from file pointed to by ${var}_FILE, or directly from var.
	"""
	if '{}_FILE'.format(var) in os.environ:
		with open(os.environ['{}_FILE'.format(var)]) as f:
			return f.read().strip()
	else:
		return os.environ[var]

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
	
	user = 'ampel'
	password = docker_env('POSTGRES_PASSWORD')
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
	
