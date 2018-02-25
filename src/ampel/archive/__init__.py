
from sqlalchemy import Table, MetaData, Column, ForeignKey, ForeignKeyConstraint
from sqlalchemy import String, Integer, BigInteger, Float
from sqlalchemy import select, and_
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import os, json
import fastavro

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
        self._meta.create_all(engine)
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

    types = {'float': Float(32), 'double': Float(64), 'int': Integer(), 'long': BigInteger(), 'string': String(255)}
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
        Column('candid', Integer(), primary_key=True, nullable=False),
        Column('objectId', String(12), nullable=False),
        Column('processor_id', Integer(), nullable=False),
        Column('ingestion_time', BigInteger(), nullable=False)
    )

    indices = {'candid', 'pid'}
    columns = filter(lambda c: c.name not in indices, map(make_column, fields['candidate']['type']['fields']))
    PhotoPoint = Table('photopoint', meta,
        Column('candid', BigInteger(), primary_key=True, nullable=False),
        Column('pid', BigInteger(), primary_key=True, nullable=False),
        
        *columns
    )

    Pivot = Table('alert_photopoint_pivot', meta,
        Column('alert_id', BigInteger(), ForeignKey("alert.candid"), primary_key=True, nullable=False),
        Column('candid', BigInteger(), primary_key=True, nullable=False),
        Column('pid', BigInteger(), primary_key=True, nullable=False),
        Column('index', Integer(), nullable=False),
        ForeignKeyConstraint(['candid', 'pid'], ['photopoint.candid', 'photopoint.pid']),
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
    with connection.begin() as transaction:
        try:
            connection.execute(meta.tables['alert'].insert(),
                candid=alert_id, objectId=alert['objectId'],
                processor_id=processor_id, ingestion_time=ingestion_time)
        except IntegrityError:
            # abort on duplicate alerts
            return
        
        # entries in prv_candidates will often be duplicated, but may also
        # be updated without warning.
        photopoint = meta.tables['photopoint']
        for idx, candidate in enumerate([alert['candidate']] + alert['prv_candidates']):
            condition = and_(photopoint.columns.pid == candidate['pid'], photopoint.columns.candid == candidate['candid'])
            
            result = connection.execute(select([photopoint.c.pid]).where(condition)).first()
            if result is None:
                # photopoint is distinct from known entries; insert it
                photopoint_id = connection.execute(photopoint.insert(), **candidate)
            connection.execute(meta.tables['alert_photopoint_pivot'].insert(), alert_id=alert_id, candid=candidate['candid'], pid=candidate['pid'], index=idx)

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
    alert['prv_candidates'] = []
    photopoint = meta.tables['photopoint']
    pivot = meta.tables['alert_photopoint_pivot']
    for result in connection.execute(select([photopoint]) \
         .select_from(photopoint.join(pivot)) \
         .where(pivot.c.alert_id == alert_id) \
         .order_by(pivot.c.index)).fetchall():
        candidate = dict(result)
        if candidate['candid'] == alert_id:
            alert['candidate'] = candidate
        else:
            # remove keys that should not appear in prv_candidates
            # FIXME: store these in a separate table
            for k in ('jdendhist', 'jdstarthist', 'ncovhist', 'ndethist', 'sgmag', 'sgscore', 'simag', 'srmag', 'szmag'):
                del candidate[k]
            alert['prv_candidates'].append(candidate)
    
    return alert
