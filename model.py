__author__ = 'Ratchasak Ranron <ratchasak.ranron@gmail.com>'
from google.appengine.ext import ndb
from flask import Flask
app = Flask(__name__)


class RMS(ndb.Model):
    timestamp = ndb.IntegerProperty(required=True)
    pu1 = ndb.IntegerProperty(required=True)
    pu2 = ndb.IntegerProperty(required=True)
    pu3 = ndb.IntegerProperty(required=True)

    @classmethod
    def query_rms(cls, start, end, count, is_asc):
        query = cls.query()
        # set filter
        if start is not None:
            query = query.filter(cls.timestamp >= start)
        if end is not None:
            query = query.filter(cls.timestamp <= end)

        # set order
        if is_asc is True:
            query = query.order(cls.timestamp)
        else:
            query = query.order(-cls.timestamp)

        # set limit
        if count is not None:
            query = query.fetch(limit=count)

        return query


class RMSResolution(ndb.Model):
    timestamp = ndb.IntegerProperty(required=True)
    period_sag1 = ndb.IntegerProperty(required=True)
    period_sag2 = ndb.IntegerProperty(required=True)
    period_sag3 = ndb.IntegerProperty(required=True)
    total_sag1 = ndb.IntegerProperty(required=True)
    total_sag2 = ndb.IntegerProperty(required=True)
    total_sag3 = ndb.IntegerProperty(required=True)

    @classmethod
    def query_rms(cls, start, end, count, is_asc):
        query = cls.query()
        # set filter
        if start is not None:
            query = query.filter(cls.timestamp >= start)
        if end is not None:
            query = query.filter(cls.timestamp <= end)

        # set order
        if is_asc is True:
            query = query.order(cls.timestamp)
        else:
            query = query.order(-cls.timestamp)

        # set limit
        if count is not None:
            query = query.fetch(limit=count)

        return query


class RMSMinute(RMSResolution):
    period_sag1 = ndb.IntegerProperty(required=True)
    period_sag2 = ndb.IntegerProperty(required=True)
    period_sag3 = ndb.IntegerProperty(required=True)
    total_sag1 = ndb.IntegerProperty(required=True)
    total_sag2 = ndb.IntegerProperty(required=True)
    total_sag3 = ndb.IntegerProperty(required=True)


class RMSHour(RMSResolution):
    period_sag1 = ndb.IntegerProperty(required=True)
    period_sag2 = ndb.IntegerProperty(required=True)
    period_sag3 = ndb.IntegerProperty(required=True)
    total_sag1 = ndb.IntegerProperty(required=True)
    total_sag2 = ndb.IntegerProperty(required=True)
    total_sag3 = ndb.IntegerProperty(required=True)


class RMSDay(RMSResolution):
    period_sag1 = ndb.IntegerProperty(required=True)
    period_sag2 = ndb.IntegerProperty(required=True)
    period_sag3 = ndb.IntegerProperty(required=True)
    total_sag1 = ndb.IntegerProperty(required=True)
    total_sag2 = ndb.IntegerProperty(required=True)
    total_sag3 = ndb.IntegerProperty(required=True)


class RMSMonth(RMSResolution):
    period_sag1 = ndb.IntegerProperty(required=True)
    period_sag2 = ndb.IntegerProperty(required=True)
    period_sag3 = ndb.IntegerProperty(required=True)
    total_sag1 = ndb.IntegerProperty(required=True)
    total_sag2 = ndb.IntegerProperty(required=True)
    total_sag3 = ndb.IntegerProperty(required=True)


class RMSYear(RMSResolution):
    period_sag1 = ndb.IntegerProperty(required=True)
    period_sag2 = ndb.IntegerProperty(required=True)
    period_sag3 = ndb.IntegerProperty(required=True)
    total_sag1 = ndb.IntegerProperty(required=True)
    total_sag2 = ndb.IntegerProperty(required=True)
    total_sag3 = ndb.IntegerProperty(required=True)


class Email(ndb.Model):
    email = ndb.StringProperty(required=True)
    enable = ndb.BooleanProperty(required=True)
    percent = ndb.IntegerProperty(required=True)


class LastTS(ndb.Model):
    lastts=ndb.IntegerProperty()