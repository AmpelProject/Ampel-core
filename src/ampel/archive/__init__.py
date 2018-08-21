#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from sqlalchemy import Table, MetaData, Column, ForeignKey, ForeignKeyConstraint, UniqueConstraint, Index
from sqlalchemy import String, Integer, BigInteger, Float, Boolean, LargeBinary, Enum
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy import select, and_, bindparam, exists
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import tuple_, func
from functools import partial
import sqlalchemy
import os, json
import fastavro
import collections

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from distutils.version import LooseVersion

class ArchiveDB(object):
    def __init__(self, *args, **kwargs):
        """
        Initialize and connect to archive database. Arguments will be passed on
        to :py:func:`sqlalchemy.create_engine`.
        """
        engine = create_engine(*args, **kwargs)
        self._meta = MetaData()
        self._meta.reflect(bind=engine)
        self._connection = engine.connect()

        Versions = self._meta.tables['versions']
        with self._connection.begin() as transaction:
            try:
                self._alert_version = LooseVersion(self._connection.execute(select([Versions.c.alert_version]).order_by(Versions.c.version_id.desc()).limit(1)).first()[0])
            finally:
                transaction.commit()
        
        alert_query, history_query, cutout_query = self._build_queries(self._meta)
        self._alert_query = alert_query
        self._history_query = history_query
        self._cutout_query = cutout_query

    def insert_alert(self, alert, schema, partition_id, ingestion_time):
        """
        Insert an alert into the archive database
    
        :param alert: alert dict
        :param schema: avro schema dictionary
        :param partition_id: the index of the Kafka partition this alert came from
        :param ingestion_time: time the alert was received, in UNIX epoch microseconds
        """
        if LooseVersion(schema['version']) > self._alert_version:
            raise ValueError("alert schema ({}) is newer than database schema ({})".format(schema['version'], self._alert_version))

        with self._connection.begin() as transaction:
            Alert = self._meta.tables['alert']
            Candidate = self._meta.tables['candidate']
            try:
                result = self._connection.execute(Alert.insert(),
                    programid=alert['candidate']['programid'], jd=alert['candidate']['jd'],
                    partition_id=partition_id, ingestion_time=ingestion_time, **alert)
                alert_id = result.inserted_primary_key[0]
                self._connection.execute(Candidate.insert(), alert_id=alert_id, **alert['candidate'])
            except IntegrityError:
                # abort on duplicate alerts
                transaction.rollback()
                return False

            # insert cutouts if they exist
            prefix = 'cutout'
            cutouts = [dict(kind=k[len(prefix):].lower(), stampData=v['stampData'], alert_id=alert_id) for k,v in alert.items() if k.startswith(prefix) if v is not None]
            if len(cutouts) > 0:
                self._connection.execute(self._meta.tables['cutout'].insert(), cutouts)
    
            if alert['prv_candidates'] is None or len(alert['prv_candidates']) == 0:
                return True
    
            # entries in prv_candidates will often be duplicated, but may also
            # be updated without warning. sort these into detections (which
            # come with unique ids) and upper limits (which don't)
            detections = []
            upper_limits = []
            for index, c in enumerate(alert['prv_candidates']):
                # entries with no candid are nondetections
                if c['candid'] is None:
                    upper_limits.append(c)
                else:
                    detections.append(c)
            for rows, label in ((detections, 'prv_candidate'), (upper_limits, 'upper_limit')):
                if len(rows) > 0:
                    self._update_history(label, rows, alert_id)

            transaction.commit()
        return True

    def _update_history(self, label, rows, alert_id):
        # insert the rows if needed
        history = self._meta.tables[label]
        self._connection.execute(postgresql.insert(history).on_conflict_do_nothing(), rows)

        # build a condition that selects the rows (just inserted or already existing)
        identifiers = next(filter(lambda c: isinstance(c, UniqueConstraint), history.constraints)).columns
        keys = [[r[c.name] for c in identifiers] for r in rows]
        target = tuple_(*identifiers).in_(keys)

        # collect the ids of the rows in an array and insert into the bridge table
        bridge = self._meta.tables['alert_{}_pivot'.format(label)]
        source = select([bindparam('alert_id'), func.array_agg(history.columns['{}_id'.format(label)])]).where(target)
        self._connection.execute(bridge.insert().from_select(bridge.columns, source), alert_id=alert_id)

    def get_statistics(self):
        stats = {}
        with self._connection.begin() as transaction:
            try:
                sql = "select relname, n_live_tup from pg_catalog.pg_stat_user_tables"
                rows = dict(self._connection.execute(sql).fetchall())
                sql = """SELECT TABLE_NAME, index_bytes, toast_bytes, table_bytes
                         FROM (
                         SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
                             SELECT c.oid,nspname AS table_schema, relname AS TABLE_NAME
                                     , c.reltuples AS row_estimate
                                     , pg_total_relation_size(c.oid) AS total_bytes
                                     , pg_indexes_size(c.oid) AS index_bytes
                                     , pg_total_relation_size(reltoastrelid) AS toast_bytes
                                 FROM pg_class c
                                 LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                                 WHERE relkind = 'r' AND nspname = 'public'
                         ) a
                       ) a;"""
                for row in self._connection.execute(sql):
                    table = {k:v for k,v in dict(row).items() if v is not None}
                    k = table.pop('table_name')
                    table['rows'] = rows[k]
                    stats[k] = table
            finally:
                transaction.commit()
        return stats
    
    @staticmethod
    def _build_queries(meta):
        PrvCandidate = meta.tables['prv_candidate']
        UpperLimit = meta.tables['upper_limit']
        Candidate = meta.tables['candidate']
        Alert = meta.tables['alert']
        Pivot = meta.tables['alert_prv_candidate_pivot']
        UpperLimitPivot = meta.tables['alert_upper_limit_pivot']
        Cutout = meta.tables['cutout']
        from sqlalchemy.sql import null
        from sqlalchemy.sql.functions import array_agg
        from sqlalchemy.sql.expression import func
        unnest = func.unnest

        def without_keys(table):
            keys = set(table.primary_key.columns)
            for fk in table.foreign_keys:
                keys.update(fk.constraint.columns)
            return [c for c in table.columns if c not in keys]

        alert_query = \
            select([Alert.c.alert_id, Alert.c.objectId, Alert.c.schemavsn] + without_keys(Candidate))\
            .select_from(Alert.join(Candidate))

        # build a query for detections
        cols = without_keys(PrvCandidate)
        # unpack the array of keys from the bridge table in order to perform a normal join
        bridge = select([Pivot.c.alert_id,
                         unnest(Pivot.c.prv_candidate_id).label('prv_candidate_id')]).alias('prv_bridge')
        prv_query = \
            select(cols) \
            .select_from(PrvCandidate.join(bridge, PrvCandidate.c.prv_candidate_id == bridge.c.prv_candidate_id)) \
            .where(bridge.c.alert_id == bindparam('alert_id'))

        # and a corresponding one for upper limits, padding out missing columns
        # with null. Note that the order of the columns must be the same, for
        # the union query below to map the result correctly to output keys.
        cols = []
        for c in without_keys(PrvCandidate):
            if c.name in UpperLimit.columns:
                cols.append(UpperLimit.columns[c.name])
            else:
                cols.append(null().label(c.name))
        # unpack the array of keys from the bridge table in order to perform a normal join
        bridge = select([UpperLimitPivot.c.alert_id,
                         unnest(UpperLimitPivot.c.upper_limit_id).label('upper_limit_id')]).alias('ul_bridge')
        ul_query = \
            select(cols) \
            .select_from(UpperLimit.join(bridge, UpperLimit.c.upper_limit_id == bridge.c.upper_limit_id)) \
            .where(bridge.c.alert_id == bindparam('alert_id'))

        # unify!
        history_query = prv_query.union(ul_query)

        cutout_query = select([Cutout.c.kind, Cutout.c.stampData]).where(Cutout.c.alert_id == bindparam('alert_id'))

        return alert_query, history_query, cutout_query

    def _fetch_alerts_with_condition(self, condition, order=None,
        with_history=True, with_cutouts=False):

        alert_query = self._alert_query.where(condition).order_by(order)

        with self._connection.begin() as transaction:
            try:
                for result in self._connection.execute(alert_query):
                    candidate = dict(result)
                    alert_id = candidate.pop('alert_id')
                    alert = {'candid': candidate['candid'], 'publisher': 'ampel'}
                    for k in 'objectId', 'schemavsn':
                        alert[k] = candidate.pop(k)
                    alert['candidate'] = {'programpi': None, 'pdiffimfilename': None, **candidate}
                    alert['prv_candidates'] = []
                    if with_history:
                        for result in self._connection.execute(self._history_query, alert_id=alert_id):
                            alert['prv_candidates'].append({'programpi': None, 'pdiffimfilename': None, **result})

                        alert['prv_candidates'] = sorted(alert['prv_candidates'], key=lambda c: (c['jd'],  c['candid'] is None, c['candid']))
                    if with_cutouts:
                        for result in self._connection.execute(self._cutout_query, alert_id=alert_id):
                            alert['cutout{}'.format(result['kind'].title())] = \
                                {'stampData': result['stampData'], 'fileName': 'unknown'}

                    yield alert
            finally:
                transaction.commit()

    def get_alert(self, candid, with_history=True, with_cutouts=False):
        """
        Retrieve an alert from the archive database
    
        :param candid: `candid` of the alert to retrieve
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :returns: the target alert as a :py:class:`dict`, or `None` if the alert is
                  not in the archive
        """
        Alert = self._meta.tables['alert']

        for alert in self._fetch_alerts_with_condition(
            Alert.c.candid == candid,
            with_history=with_history, with_cutouts=with_cutouts):
            return alert
        return None

    def get_cutout(self, candid):
        Alert = self._meta.tables['alert']
        Cutout = self._meta.tables['cutout']
        q = select([Cutout.c.kind, Cutout.c.stampData]).select_from(Cutout.join(Alert)).where(Alert.c.candid == candid)
        return dict(self._connection.execute(q).fetchall())

    def get_alerts_for_object(self, objectId, jd_start=-float('inf'), jd_end=float('inf'), with_history=False, with_cutouts=False):
        """
        Retrieve alerts from the archive database by ID
    
        :param connection: database connection
        :param meta: schema metadata
        :param objectId: id of the transient, e.g. ZTF18aaaaaa, or a collection thereof
        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :returns: a generator of alerts matching the condition
        """
        Alert = self._meta.tables['alert']
        if isinstance(objectId, str):
            match = Alert.c.objectId == objectId
        elif isinstance(objectId, collections.Collection):
            match = in_(Alert.c.objectId, objectId)
        else:
            raise TypeError("objectId must be str or collection, got {}".format(type(objectId)))
        in_range = and_(Alert.c.jd >= jd_start, Alert.c.jd < jd_end, match)

        yield from self._fetch_alerts_with_condition(
            in_range, Alert.c.jd.asc(),
            with_history=with_history, with_cutouts=with_cutouts)

    def get_alerts(self, candids, with_history=True, with_cutouts=False):
        """
        Retrieve alerts from the archive database by ID
    
        :param alert_id: a collection of `candid` of alerts to retrieve
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        :returns: a generator of alerts matching the condition
        """
        Alert = self._meta.tables['alert']
        # mimic mysql field() function, passing the order by hand
        order = sqlalchemy.text(','.join(('alert.candid=%d DESC' % i for i in candids)))

        yield from self._fetch_alerts_with_condition(Alert.c.candid.in_(candids), order,
            with_history=with_history, with_cutouts=with_cutouts)

    def get_alerts_in_time_range(self, jd_min, jd_max, partitions=None, programid=None, with_history=True, with_cutouts=False):
        """
        Retrieve a range of alerts from the archive database

        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start
        :param partitions: range of partitions to consume. Clients with disjoint
            partitions will not receive duplicate alerts even if they request
            overlapping time ranges.
        :type partitions: int or slice
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        """
        Alert = self._meta.tables['alert']
        in_range = and_(Alert.c.jd >= jd_min, Alert.c.jd < jd_max)
        if isinstance(partitions, int):
            in_range = and_(in_range, Alert.c.partition_id == partitions)
        elif isinstance(partitions, slice):
            assert partitions.step == 1 or partitions.step is None
            in_range = and_(in_range, and_(Alert.c.partition_id >= partitions.start, Alert.c.partition_id < partitions.stop))
        elif partitions is not None:
            raise TypeError("partitions must be int or slice")
        if isinstance(programid, int):
            in_range = and_(in_range, Alert.c.programid == programid)

        yield from self._fetch_alerts_with_condition(
            in_range, Alert.c.jd.asc(),
            with_history=with_history, with_cutouts=with_cutouts)

    def get_alerts_in_cone(self, ra, dec, radius, jd_min=None, jd_max=None, with_history=False, with_cutouts=False):
        """
        Retrieve a range of alerts from the archive database

        :param ra: right ascension of search field center in degrees (J2000)
        :param dec: declination of search field center in degrees (J2000)
        :param radius: radius of search field in degrees
        :param jd_start: minimum JD of exposure start
        :param jd_end: maximum JD of exposure start
        :param with_history: return alert with previous detections and upper limits
        :param with_cutout: return alert with cutout images
        
        """
        from sqlalchemy import func
        from sqlalchemy.sql.expression import BinaryExpression
        Alert = self._meta.tables['alert']
        Candidate = self._meta.tables['candidate']
    
        center = func.ll_to_earth(dec, ra)
        box = func.earth_box(center, radius)
        loc = func.ll_to_earth(Candidate.c.dec, Candidate.c.ra)
    
        in_range = and_(BinaryExpression(box, loc, '@>'), func.earth_distance(center, loc) < radius)
        # NB: filtering on jd from Candidate here is ~2x faster than _also_
        #      filtering on Alert (rows that pass are joined on the indexed
        #      primary key)
        if jd_min is not None:
            in_range = and_(in_range, Candidate.c.jd >= jd_min)
        if jd_max is not None:
            in_range = and_(in_range, Candidate.c.jd < jd_max)

        yield from self._fetch_alerts_with_condition(
            in_range, Alert.c.jd.asc(),
            with_history=with_history, with_cutouts=with_cutouts)
