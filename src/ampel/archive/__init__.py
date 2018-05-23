
from sqlalchemy import Table, MetaData, Column, ForeignKey, ForeignKeyConstraint, Index
from sqlalchemy import String, Integer, BigInteger, Float, Boolean
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy import select, and_, bindparam, exists
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
        return insert_alert(self._connection, self._meta, alert, partition_id, ingestion_time)

    def get_statistics(self):
        sql = "select relname, n_live_tup from pg_catalog.pg_stat_user_tables"
        return dict(self._connection.execute(sql).fetchall())
    
    def get_alert(self, alert_id):
        return get_alert(self._connection, self._meta, alert_id)

    def get_alerts_for_object(self, objectId):
        return get_alerts_for_object(self._connection, self._meta, objectId)

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
    #types = {'float': Float(), 'double': double, 'int': Integer(), 'long': BigInteger(), 'string': String(255)}
    types = {'float': Float(), 'double': double, 'int': Integer(), 'long': BigInteger()}
    def make_column(field):
        kwargs = {}
        if type(field['type']) is list:
            typename = field['type'][0]
            if field['type'][1] == 'null':
                kwargs['nullable'] = True
        else:
            typename = field['type']
        if field['name'] == 'isdiffpos':
            type_ = Boolean()
        elif typename in types:
            type_ = types[typename]
        else:
            raise ValueError("Unknown field type '{}'".format(typename))
        
        return Column(field['name'], type_, **kwargs)

    meta = MetaData()

    Alert = Table('alert', meta,
        Column('alert_id', Integer(), primary_key=True, nullable=False, autoincrement=True),
        Column('candid', BigInteger(), nullable=False),
        Column('programid', Integer(), nullable=False),
        Column('objectId', String(12), nullable=False),
        Column('partition_id', Integer(), nullable=False),
        Column('ingestion_time', BigInteger(), nullable=False),
        Column('jd', double, nullable=False),
        Index('alert_playback', 'partition_id', 'jd'),
        Index('alert_insertion', 'candid', 'programid'),
    )

    skip = ['pdiffimfilename', 'programpi', 'ssnamenr']
    identifiers = ['candid', 'programid', 'pid']
    columns = map(make_column, filter(lambda f: f['name'] not in skip, fields['candidate']['type']['fields']))
    Table('candidate', meta,
        Column('candidate_id', Integer(), primary_key=True, nullable=False, autoincrement=True),
        Column('alert_id', Integer(), ForeignKey('alert.alert_id'), nullable=False),
        
        *columns
    )
    
    skip = ['pdiffimfilename', 'programpi', 'ssnamenr']
    identifiers = ['candid', 'programid', 'pid']
    columns = list(map(make_column, filter(lambda f: f['name'] not in skip, fields['prv_candidates']['type'][0]['items']['fields'])))
    columns.append(Index('unique_prv_candidate', *identifiers))
    Table('prv_candidate', meta,
        Column('prv_candidate_id', Integer(), primary_key=True, nullable=False, autoincrement=True),
        
        *columns
    )

    Table('alert_prv_candidate_pivot', meta,
        Column('alert_id', Integer(), ForeignKey('alert.alert_id'), primary_key=True, nullable=False),
        Column('prv_candidate_id', Integer(), ForeignKey('prv_candidate.prv_candidate_id'), primary_key=True, nullable=False),
    )

    identifiers = ['jd', 'fid', 'pid', 'diffmaglim']
    fluff = ['programid']
    columns = list(map(make_column, filter(lambda f: f['name'] in identifiers+fluff, fields['prv_candidates']['type'][0]['items']['fields'])))
    columns.append(Index('unique_upper_limit', *identifiers))
    Table('upper_limit', meta,
        Column('upper_limit_id', Integer(), primary_key=True, autoincrement=True),
        *columns
    )

    Table('alert_upper_limit_pivot', meta,
        Column('alert_id', Integer(), ForeignKey('alert.alert_id'), primary_key=True, nullable=False),
        Column('upper_limit_id', Integer(), ForeignKey('upper_limit.upper_limit_id'), primary_key=True, nullable=False),
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

def _convert_isdiffpos(in_dict):
    out_dict = dict(in_dict)
    k = 'isdiffpos'
    v = out_dict[k]
    if k is not None:
        out_dict[k] = v in {'0', 't'}
    return out_dict

def _insert_unless_exists(index, returning=True):
    Table = index.table
    index_cols = index.columns
    target = and_(*(c == bindparam(c.name) for c in index_cols))
    idcol = Table.columns['{}_id'.format(Table.name)]
    query = Table.insert().from_select(
        [c for c in Table.columns if c != idcol],
        select([bindparam(k) for k in Table.columns if k.name != idcol.name]) \
            .where(~exists(select([idcol]).where(target)))
    )
    if returning:
        return query.returning(idcol)
    else:
        return query

def _update_bridge(index, bridge):
    target = and_(*(c == bindparam(c.name) for c in index.columns))
    source_key = list(bridge.columns)[0].name
    idcol = index.table.columns['{}_id'.format(index.table.name)]
    query = bridge.insert().from_select(bridge.columns, select([bindparam(source_key), idcol]).where(target))
    return query

def insert_alert(connection, meta, alert, partition_id, ingestion_time):
    """
    Insert an alert into the archive database
    
    :param connection: database connection
    :param meta: schema metadata
    :param alert: alert dict
    :param partition_id: the index of the Kafka partition this alert came from
    :param ingestion_time: time the alert was received, in UNIX epoch microseconds
    
    """
    with connection.begin() as transaction:
        Alert = meta.tables['alert']
        Candidate = meta.tables['candidate']
        try:
            index = next(((idx for idx in Alert.indexes if idx.name == "alert_insertion")))
            result = connection.execute(_insert_unless_exists(index),
                candid=alert['candid'],  programid=alert['candidate']['programid'], objectId=alert['objectId'],
                partition_id=partition_id, ingestion_time=ingestion_time,
                jd=alert['candidate']['jd'])
            tup = result.fetchone()
            if tup is None:
                transaction.rollback()
                return False
            alert_id = tup[0]
            connection.execute(Candidate.insert(), alert_id=alert_id, **_convert_isdiffpos(alert['candidate']))
        except IntegrityError:
            # abort on duplicate alerts
            transaction.rollback()
            return False
    
        if alert['prv_candidates'] is None or len(alert['prv_candidates']) == 0:
            return True
    
        # entries in prv_candidates will often be duplicated, but may also
        # be updated without warning. sort these into detections (which come with
        # unique ids) and upper limits (which don't)
        detections = dict(rows=[], pivots=[])
        upper_limits = dict(rows=[], pivots=[])
        for index, c in enumerate(alert['prv_candidates']):
            # entries with no candid are nondetections
            if c['candid'] is None:
                upper_limits['rows'].append(c)
                upper_limits['pivots'].append(dict(alert_id=alert_id, **c))
            else:
                detections['rows'].append(_convert_isdiffpos(c))
                detections['pivots'].append(dict(alert_id=alert_id, **c))
    
        if len(detections['rows']) > 0:
            ignore = {'{}_ignore_duplicates'.format(connection.dialect.name): True}
            index = list(meta.tables['prv_candidate'].indexes)[0]
            bridge = meta.tables['alert_prv_candidate_pivot']
            connection.execute(_insert_unless_exists(index, False), detections['rows'])
            connection.execute(_update_bridge(index, bridge), detections['pivots'])
    
        if len(upper_limits['rows']) > 0:
            index = list(meta.tables['upper_limit'].indexes)[0]
            bridge = meta.tables['alert_upper_limit_pivot']
            connection.execute(_insert_unless_exists(index, False), upper_limits['rows'])
            connection.execute(_update_bridge(index, bridge), upper_limits['pivots'])
    
        transaction.commit()
    return True

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

def get_alerts_for_object(connection, meta, objectId):
    """
    Retrieve alerts from the archive database by ID
    
    :param connection: database connection
    :param meta: schema metadata
    :param alert_id: a collection of `candid` of alerts to retrieve
    :returns: the target alert as a :py:class:`dict`, or `None` if the alert is
              not in the archive
    """
    Alert = meta.tables['alert']

    yield from fetch_alerts_with_condition(connection, meta, Alert.c.objectId == objectId, Alert.c.jd.asc())

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

def _build_queries(meta):
    PrvCandidate = meta.tables['prv_candidate']
    UpperLimit = meta.tables['upper_limit']
    Candidate = meta.tables['candidate']
    Alert = meta.tables['alert']
    Pivot = meta.tables['alert_prv_candidate_pivot']
    UpperLimitPivot = meta.tables['alert_upper_limit_pivot']
    from sqlalchemy.sql import null

    def without_keys(table):
        keys = set(table.primary_key.columns)
        for fk in table.foreign_keys:
            keys.update(fk.constraint.columns)
        return [c for c in table.columns if c not in keys]

    alert_query = \
        select([Alert.c.alert_id, Alert.c.objectId] + without_keys(Candidate))\
        .select_from(Alert.join(Candidate))

    # build a query for detections
    cols = without_keys(PrvCandidate)
    prv_query = \
        select(cols) \
        .select_from(PrvCandidate.join(Pivot)) \
        .where(Pivot.c.alert_id == bindparam('alert_id'))

    # and a corresponding one for upper limits, padding out missing columns
    # with null. Note that the order of the columns must be the same, for
    # the union query below to map the result correctly to output keys.
    cols = []
    for c in without_keys(PrvCandidate):
        if c.name in UpperLimit.columns:
            cols.append(UpperLimit.columns[c.name])
        else:
            cols.append(null().label(c.name))
    ul_query = \
        select(cols) \
        .select_from(UpperLimit.join(UpperLimitPivot)) \
        .where(UpperLimitPivot.c.alert_id == bindparam('alert_id'))

    # unify!
    history_query = prv_query.union(ul_query)

    return alert_query, history_query

def fetch_alerts_with_condition(connection, meta, condition, order=None):

    alert_query, history_query = _build_queries(meta)

    alert_query = alert_query.where(condition).order_by(order)

    for result in connection.execute(alert_query):
        candidate = dict(result)
        alert_id = candidate.pop('alert_id')
        alert = dict(objectId=candidate.pop('objectId'), candid=candidate['candid'])
        alert['candidate'] = candidate
        alert['prv_candidates'] = []
        for result in connection.execute(history_query, alert_id=alert_id):
            alert['prv_candidates'].append(dict(result))

        alert['prv_candidates'] = sorted(alert['prv_candidates'], key=lambda c: c['jd'])

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
	parser.add_argument('--drop', action="store_true", default=False)
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
	if opts.drop:
		meta.drop_all(engine)
	meta.create_all(engine)
	
