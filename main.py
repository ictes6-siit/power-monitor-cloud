"""`main` is the top level module for your Flask application."""

# Import the Flask Framework
from google.appengine.ext import ndb
from google.appengine.api import mail
from google.appengine.api import taskqueue
from flask import Flask,jsonify
from flask import request
from crossdomain import crossdomain
from model import *
import datetime
import logging
app = Flask(__name__)

@app.route('/clear', methods=['GET'])
def get_clear():
    ndb.delete_multi(RMS.query().iter(keys_only = True))
    ndb.delete_multi(RMSMinute.query().iter(keys_only = True))
    ndb.delete_multi(RMSHour.query().iter(keys_only = True))
    ndb.delete_multi(RMSDay.query().iter(keys_only = True))
    ndb.delete_multi(RMSMonth.query().iter(keys_only = True))
    ndb.delete_multi(RMSYear.query().iter(keys_only = True))
    ndb.delete_multi(Email.query().iter(keys_only = True))
    ndb.delete_multi(LastTS.query().iter(keys_only = True))
    return jsonify({'status': 'OK'}), 200

@app.route('/rms.json', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*', headers='Origin, X-Requested-With, Content-Type, Accept')
def post_rms():
    logging.debug('timestamp post: %d' % request.json['timestamp'])
    taskqueue.add(url='/rms_queue', params={
        'pu1': request.json['pu1'],
        'pu2': request.json['pu2'],
        'pu3': request.json['pu3'],
        'timestamp': request.json['timestamp']})

    return jsonify({'status': 'OK'}), 201


@app.route('/rms_queue', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*', headers='Origin, X-Requested-With, Content-Type, Accept')
def rms_queue():
    logging.debug('timestamp queue: %d' % int(request.values['timestamp']))
    pu1 = (100 - int(request.values['pu1'])) if (int(request.values['pu1']) <= 100) else 100
    pu2 = (100 - int(request.values['pu2'])) if (int(request.values['pu2']) <= 100) else 100
    pu3 = (100 - int(request.values['pu3'])) if (int(request.values['pu3']) <= 100) else 100
    this_rms = RMS(
        pu1=pu1,
        pu2=pu2,
        pu3=pu3,
        timestamp=int(request.values['timestamp']))

    # push to other resolution
    # get last rms
    prev_rms = RMS.query_rms(None, None, 1, False)
    if prev_rms is not None and len(prev_rms) > 0:
        prev_rms = prev_rms[0]
        # check for sag
        if prev_rms.pu1 <= 90 or prev_rms.pu2 <= 90 or prev_rms.pu3 <= 90:
            sag_time_all = int(this_rms.timestamp) - int(prev_rms.timestamp)

            # add sag avg in first block
            sag_time_can = (prev_rms.timestamp - (prev_rms.timestamp % 60000)) + 60000 - prev_rms.timestamp
            if sag_time_all < sag_time_can:
                sag_time = sag_time_all
            else:
                sag_time = sag_time_can
            minute_block_from = prev_rms.timestamp - (prev_rms.timestamp % 60000)
            minute_block_to = minute_block_from + 60000 - 1
            rms_minute_query = RMSMinute.query_rms(minute_block_from, minute_block_to, None, True)

            if sag_time < 0:
                    logging.debug('neg sag: %d, %d, %d, %d, %d' % (sag_time_all, sag_time_can, sag_time, int(this_rms.timestamp), int(prev_rms.timestamp)))
            if rms_minute_query.count() > 0:
                # already have the average, use previous average to calculate next avg
                prev_avg = rms_minute_query.get()
                # calculate next avg
                prev_avg.period_sag1 += (sag_time if prev_rms.pu1 <= 90 else 0)
                prev_avg.period_sag2 += (sag_time if prev_rms.pu2 <= 90 else 0)
                prev_avg.period_sag3 += (sag_time if prev_rms.pu3 <= 90 else 0)
                prev_avg.total_sag1 += (1 if prev_rms.pu1 <= 90 else 0)
                prev_avg.total_sag2 += (1 if prev_rms.pu2 <= 90 else 0)
                prev_avg.total_sag3 += (1 if prev_rms.pu3 <= 90 else 0)
                prev_avg.put()
            else:
                # don't have average
                new_rms_minute = RMSMinute(
                    timestamp=minute_block_from,
                    period_sag1=(sag_time if prev_rms.pu1 <= 90 else 0),
                    period_sag2=(sag_time if prev_rms.pu2 <= 90 else 0),
                    period_sag3=(sag_time if prev_rms.pu3 <= 90 else 0),
                    total_sag1=(1 if prev_rms.pu1 <= 90 else 0),
                    total_sag2=(1 if prev_rms.pu2 <= 90 else 0),
                    total_sag3=(1 if prev_rms.pu3 <= 90 else 0))
                new_rms_minute.put()
                logging.debug('create minute, this %d' % minute_block_from)

            # fill next overlap block
            sag_time_left = sag_time_all - sag_time
            next_block_pointer = minute_block_from + 60000
            while sag_time_left > 0:
                if sag_time_left > 60000:
                    sag_time_this_block = 60000
                else:
                    sag_time_this_block = sag_time_left

                rms_minute_query = RMSMinute.query_rms(next_block_pointer, next_block_pointer + 60000 - 1, None, True)

                if sag_time_this_block < 0:
                    logging.debug('neg sag 2: %d' % sag_time_this_block)
                if rms_minute_query.count() > 0:
                    # already have the average, use previous average to calculate next avg
                    prev_avg = rms_minute_query.get()
                    # calculate next avg
                    prev_avg.period_sag1 += (sag_time_this_block if prev_rms.pu1 <= 90 else 0)
                    prev_avg.period_sag2 += (sag_time_this_block if prev_rms.pu2 <= 90 else 0)
                    prev_avg.period_sag3 += (sag_time_this_block if prev_rms.pu3 <= 90 else 0)
                    prev_avg.put()
                else:
                    new_rms_minute = RMSMinute(
                        timestamp=next_block_pointer,
                        period_sag1=(sag_time_this_block if prev_rms.pu1 <= 90 else 0),
                        period_sag2=(sag_time_this_block if prev_rms.pu2 <= 90 else 0),
                        period_sag3=(sag_time_this_block if prev_rms.pu3 <= 90 else 0),
                        total_sag1=0,
                        total_sag2=0,
                        total_sag3=0)
                    new_rms_minute.put()
                    logging.debug('update minute, next %d' % next_block_pointer)

                sag_time_left -= sag_time_this_block
                next_block_pointer += 60000

            # update hour resolution
            _update_resolution(prev_rms, this_rms, (60*60*1000), RMSMinute, RMSHour)

            # update day resolution
            _update_resolution(prev_rms, this_rms, (24*60*60*1000), RMSHour, RMSDay)

            # update month resolution
            _update_resolution(prev_rms, this_rms, (31*24*60*60*1000), RMSDay, RMSMonth)

            # update year resolution
            _update_resolution(prev_rms, this_rms, (15*31*24*60*60*1000), RMSMonth, RMSYear)

    this_rms.put()

    return jsonify({'status': 'OK'}), 201


def _update_resolution(prev_rms, this_rms, block_time, FromTable, ToTable):
    block_pointer = prev_rms.timestamp - (prev_rms.timestamp % block_time)
    rms_list = FromTable.query_rms(block_pointer,
                                   block_pointer + block_time - 1, None, False).fetch()
    while block_pointer < this_rms.timestamp:
        period_sag1, period_sag2, period_sag3, total_sag1, total_sag2, total_sag3 = _avg_rms_list(rms_list)
        rms_query = ToTable.query_rms(block_pointer,
                                         block_pointer + block_time - 1, None, False)
        if rms_query.count() > 0:
            # already have the average, use previous average to calculate next avg
            prev_avg = rms_query.get()
            prev_avg.period_sag1 = period_sag1
            prev_avg.period_sag2 = period_sag2
            prev_avg.period_sag3 = period_sag3
            prev_avg.total_sag1 = total_sag1
            prev_avg.total_sag2 = total_sag2
            prev_avg.total_sag3 = total_sag3
            prev_avg.put()
        else:
            # don't have average
            new_rms = ToTable(
                timestamp=block_pointer,
                period_sag1=period_sag1,
                period_sag2=period_sag2,
                period_sag3=period_sag3,
                total_sag1=total_sag1,
                total_sag2=total_sag2,
                total_sag3=total_sag3)
            new_rms.put()

        block_pointer += block_time
        rms_list = FromTable.query_rms(block_pointer, block_pointer + block_time - 1,
                                       None, False).fetch()


def _avg_rms_list(rms_list):
    period_sag1 = 0
    period_sag2 = 0
    period_sag3 = 0
    total_sag1 = 0
    total_sag2 = 0
    total_sag3 = 0
    length = len(rms_list)
    for i in range(0, length):
        if i < length:
            period_sag1 += rms_list[i].period_sag1
            period_sag2 += rms_list[i].period_sag2
            period_sag3 += rms_list[i].period_sag3
            total_sag1 += rms_list[i].total_sag1
            total_sag2 += rms_list[i].total_sag2
            total_sag3 += rms_list[i].total_sag3

    return period_sag1, period_sag2, period_sag3, total_sag1, total_sag2, total_sag3


@app.route('/rms.json', methods=['GET'])
@crossdomain(origin='*')
def get_rms():
    try:
        start = None if (request.args.get('start') is None) else int(request.args.get('start'))
        end = None if (request.args.get('end') is None) else int(request.args.get('end'))
        count = None if (request.args.get('count') is None) else int(request.args.get('count'))
        is_asc = True if (request.args.get('asc') is None) else (request.args.get('asc').lower() in ("true", "1"))
        is_scale = False if (request.args.get('scale') is None) else (request.args.get('scale').lower() in ("true", "1"))
    except:
        return jsonify({'status': 'Invalid input format'})

    # check time
    if start is not None and end is not None and start > end:
        return jsonify({'status': 'Invalid time'})

    if is_scale is True:
        tmp_start = start
        tmp_end = end
        if start is None:
            tmp_start = RMS.query_rms(None, None, 1, True)[0].timestamp
        if end is None:
            tmp_end = RMS.query_rms(None, None, 1, False)[0].timestamp

        selected_range = tmp_end - tmp_start

        block_time = 1
        if selected_range < 30 * 60 * 60 * 1000:               # 30 hours range
            # return 1 point per minute
            query = RMSMinute.query_rms(start, end, count, is_asc)
            block_time = 60 * 1000
        elif selected_range < 31 * 24 * 60 * 60 * 1000:          # one month range
            # return 1 point per hour
            query = RMSHour.query_rms(start, end, count, is_asc)
            block_time = 60 * 60 * 1000
        else:                                                    # > one month range
            # return 1 point per day
            query = RMSDay.query_rms(start, end, count, is_asc)
            block_time = 24 * 60 * 60 * 1000
    else:
        query = RMS.query_rms(start, end, count, is_asc)

    rms_list = []
    for rms in query:
        data = {'timestamp': rms.timestamp}
        if is_scale is True:
            data.update({'pu1': (rms.period_sag1 / float(block_time)) * 100,
                         'pu2': (rms.period_sag2 / float(block_time)) * 100,
                         'pu3': (rms.period_sag3 / float(block_time)) * 100,
                         'total_sag1': rms.total_sag1,
                         'total_sag2': rms.total_sag2,
                         'total_sag3': rms.total_sag3})
        else:
            data.update({'pu1': rms.pu1, 'pu2': rms.pu2, 'pu3': rms.pu3})
        rms_list.append(data)

    results = {'rms': rms_list}
    return jsonify({'status': 'OK', 'results': results}), 200


@app.route('/email', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*', headers='Origin, X-Requested-With, Content-Type, Accept')
def post_email():
    email = Email(
        id=request.json['email'],
        email=request.json['email'],
        percent=int(request.json['percent']),
        enable=bool(request.json['enable']))
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
    email.percent = request.json['percent']
    email.enable = request.json['enable']
    email.put()
    return jsonify({'status': 'OK'}), 201


@app.route('/email', methods=['GET'])
@crossdomain(origin='*')
def get_email():
    email_list = []
    query = Email.query()
    for email in query:
        data = {'email': email.email, 'percent': email.percent, 'enable': email.enable}
        email_list.append(data)
    results = {'email': email_list}
    return jsonify({'status': 'OK', 'results': results}), 200


@app.route('/mailnotify', methods=['get'])
def mailnotify():
    powerdata = []

    last_ts_query = 0
    querylastts = LastTS.query().order(-LastTS.lastts).fetch(1)
    if not querylastts:
        logging.info('nolist last timestamp=0')
    else:
        logging.info('get timestamp from db')
        for tmp_ts in querylastts:
            last_ts_query = tmp_ts.lastts

    data = RMS.query(RMS.timestamp > last_ts_query).order(-RMS.timestamp).fetch()

    users = Email.query().fetch()
    logging.info('len='+str(range(len(data))))
    logging.info(data)
    if data:
        for user in users:
            logging.info(user.email+" "+str(user.enable))
            sag1 = 0
            sag2 = 0
            sag3 = 0
            sendmail = 0
            body_forsend = ''
            if user.enable:
                for tmp in data:
                    task={'pu1': tmp.pu1, 'pu2': tmp.pu2, 'pu3': tmp.pu3, 'timestamp': tmp.timestamp}
                    powerdata.append(task)

                    #calculate here to send or not
                    #case1: percent change > 10
                    unix_timestamp = str(tmp.timestamp)
                    milliseconds = 0
                    time_readable = 0
                    if len(unix_timestamp) == 13:
                        milliseconds = float(str(unix_timestamp)[-3:])
                        unix_timestamp = float(str(unix_timestamp)[0:-3])

                        time_readable = datetime.datetime.fromtimestamp(unix_timestamp)
                        time_readable += datetime.timedelta(milliseconds=milliseconds)

                    if tmp.pu1 > user.percent:
                        sag1 += 1
                        body_pu1 = 'Phase 1 sag : Voltage is at %s %%\n' % tmp.pu1
                        body_forsend += body_pu1
                        sendmail = 1
                    if tmp.pu2 > user.percent:
                        sag2 += 1
                        body_pu2 = 'Phase 2 sag : Voltage is at %s %%\n' % tmp.pu2
                        body_forsend += body_pu2
                        sendmail = 1
                    if tmp.pu3 > user.percent:
                        sag3 += 1
                        body_pu3 = 'Phase 3 sag : Voltage is at %s %%\n' % tmp.pu3
                        body_forsend += body_pu3
                        sendmail = 1
                    if tmp.pu1 > user.percent or tmp.pu2 > user.percent or tmp.pu3 > user.percent:
                        body_forsend += 'at time ' + str(time_readable) + '\n\n'

                if sendmail == 0:
                    logging.info('email not send')
                elif sendmail == 1:
                    mail.send_mail(sender='Toonja 1990 <toonja1990@gmail.com>',
                    to=user.email,
                    subject='Voltage Sag Alert !',
                    body=body_forsend)
                logging.info(body_forsend)

        #remember last timestamp of sag data
        lasttimestamp = powerdata[0]
        last_ts = LastTS(lastts=lasttimestamp['timestamp'])
        last_ts.put()
    else:
        logging.info('No data to sent')

    return 'alert success'


@app.errorhandler(404)
def page_not_found(e):
    """Return a custom 404 error."""
    return 'Sorry, Nothing at this URL.', 404


@app.errorhandler(500)
def page_not_found(e):
    """Return a custom 500 error."""
    return 'Sorry, unexpected error: {}'.format(e), 500

