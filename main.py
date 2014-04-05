"""`main` is the top level module for your Flask application."""

# Import the Flask Framework
from google.appengine.ext import ndb
from flask import Flask,jsonify
from flask import abort
from flask import make_response
from flask import request
from crossdomain import crossdomain
import cgi
import urllib
app = Flask(__name__)
# Note: We don't need to call run() since our application is embedded within
# the App Engine WSGI application server.


class RMS(ndb.Model):
    timestamp = ndb.IntegerProperty(required=True)
    pu1 = ndb.IntegerProperty(required=True)
    pu2 = ndb.IntegerProperty(required=True)
    pu3 = ndb.IntegerProperty(required=True)

    @classmethod
    def del_all(cls):
        ndb.gql('DELETE * FROM RMS')
        print ndb.gql('DELETE * FROM RMS')


class Email(ndb.Model):
    email = ndb.StringProperty(required=True)
    sag = ndb.IntegerProperty(required=True)
    time = ndb.IntegerProperty(required=True)
    enabled = ndb.BooleanProperty(required=True)


@app.route('/rms', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*', headers='Origin, X-Requested-With, Content-Type, Accept')
def post_rms():
    rms = RMS(
        pu1=request.json['pu1'],
        pu2=request.json['pu2'],
        pu3=request.json['pu3'],
        timestamp=request.json['timestamp'])
    rms.put()
    return jsonify({'status': 'OK'}), 201


@app.route('/rms', methods=['GET'])
@crossdomain(origin='*')
def get_rms():
    rms_list = []
    query = RMS.query().order(RMS.timestamp)
    for rms in query:
        data = {'timestamp': rms.timestamp, 'pu1': rms.pu1, 'pu2': rms.pu2, 'pu3': rms.pu3}
        rms_list.append(data)

    results = {'rms': rms_list}
    return jsonify({'status': 'OK', 'results': results}), 200


@app.route('/rms/<int:from_time>/<int:to_time>', methods=['GET'])
@crossdomain(origin='*')
def get_rms_by_to_from_time(from_time, to_time):
    rms_list = []
    query = RMS.query(RMS.timestamp >= from_time, RMS.timestamp <= to_time).order(RMS.timestamp)
    for rms in query:
        data = {'timestamp': rms.timestamp, 'pu1': rms.pu1, 'pu2': rms.pu2, 'pu3': rms.pu3}
        rms_list.append(data)
    results = {'rms': rms_list}
    return jsonify({'status': 'OK', 'results': results}), 200


@app.route('/rms/<int:from_time>', methods=['GET'])
@crossdomain(origin='*')
def get_rms_by_from_time(from_time):
    rms_list = []
    query = RMS.query(RMS.timestamp >= from_time).order(RMS.timestamp)
    for rms in query:
        data = {'timestamp': rms.timestamp, 'pu1': rms.pu1, 'pu2': rms.pu2, 'pu3': rms.pu3}
        rms_list.append(data)
    results = {'rms': rms_list}
    return jsonify({'status': 'OK', 'results': results}), 200


@app.route('/email', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*', headers='Origin, X-Requested-With, Content-Type, Accept')
def post_email():
    email = Email(
        id=request.json['email'],
        email=request.json['email'],
        sag=int(request.json['sag']),
        time=int(request.json['time']),
        enabled=bool(request.json['enabled']))
    email.put()
    return jsonify({'status': 'OK'}), 201


@app.route('/email', methods=['DELETE'])
@crossdomain(origin='*')
def delete_email():
    email_key = ndb.Key(Email, request.json['email'])
    email_key.delete()
    return jsonify({'status': 'OK'}), 201


@app.route('/email', methods=['PUT'])
@crossdomain(origin='*')
def put_email():
    email_key = ndb.Key(Email, str(request.json['email']))
    email = email_key.get()
    email.sag = request.json['sag']
    email.time = request.json['time']
    email.enabled = request.json['enabled']
    email.put()
    return jsonify({'status': 'OK'}), 201


@app.route('/email', methods=['GET'])
@crossdomain(origin='*')
def get_email():
    email_list = []
    query = Email.query()
    for email in query:
        data = {'email': email.email, 'sag': email.sag, 'time': email.time, 'enabled': email.enabled}
        email_list.append(data)
    results = {'email': email_list}
    return jsonify({'status': 'OK', 'results': results}), 200


@app.errorhandler(404)
def page_not_found(e):
    """Return a custom 404 error."""
    return 'Sorry, Nothing at this URL.', 404


@app.errorhandler(500)
def page_not_found(e):
    """Return a custom 500 error."""
    return 'Sorry, unexpected error: {}'.format(e), 500

