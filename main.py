"""`main` is the top level module for your Flask application."""

# Import the Flask Framework
from google.appengine.ext import ndb
from google.appengine.api import mail
from flask import Flask,jsonify
from flask import request
from crossdomain import crossdomain
from model import *
import datetime
import logging
app = Flask(__name__)

@app.route('/test', methods=['GET'])
def get_test():
    ndb.delete_multi(RMSSecond.query().iter(keys_only = True))
    return jsonify({'status': 'OK'}), 200

@app.route('/rms.json', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*', headers='Origin, X-Requested-With, Content-Type, Accept')
def post_rms():
    pu1 = (100 - request.json['pu1']) if (request.json['pu1'] < 100) else 100
    pu2 = (100 - request.json['pu2']) if (request.json['pu2'] < 100) else 100
    pu3 = (100 - request.json['pu3']) if (request.json['pu3'] < 100) else 100
    this_rms = RMS(
        pu1=pu1,
        pu2=pu2,
        pu3=pu3,
        timestamp=request.json['timestamp'])

    # push to other resolution
    # get last rms
    prev_rms = RMS.query_rms(None, None, 1, False)
    if prev_rms is not None and len(prev_rms) > 0:
        prev_rms = prev_rms[0]
        # check for sag
        if prev_rms.pu1 < 90 or prev_rms.pu2 < 90 or prev_rms.pu3 < 90:
            sag_time_all = int(this_rms.timestamp) - int(prev_rms.timestamp)
            sag_pu1 = prev_rms.pu1
            sag_pu2 = prev_rms.pu2
            sag_pu3 = prev_rms.pu3
            add_sag1 = 1 if prev_rms.pu1 < 90 else 0
            add_sag2 = 1 if prev_rms.pu2 < 90 else 0
            add_sag3 = 1 if prev_rms.pu3 < 90 else 0

            # add sag avg in first block
            sag_time_can = (prev_rms.timestamp - (prev_rms.timestamp % 60000)) + 60000 - prev_rms.timestamp
            if sag_time_all < sag_time_can:
                sag_time = sag_time_all
            else:
                sag_time = sag_time_can
            minute_block_from = prev_rms.timestamp - (prev_rms.timestamp % 60000)
            minute_block_to = minute_block_from + 60000
            rms_minute_query = RMSMinute.query_rms(minute_block_from, minute_block_to, None, False)
            if rms_minute_query.count() > 0:
                # already have the average, use previous average to calculate next avg
                prev_avg = rms_minute_query.get()
                # calculate next avg
                prev_avg.pu1 = ((prev_avg.pu1 * 60000) + (sag_pu1 * sag_time) - (100 * sag_time))/60000.0
                prev_avg.pu2 = ((prev_avg.pu2 * 60000) + (sag_pu2 * sag_time) - (100 * sag_time))/60000.0
                prev_avg.pu3 = ((prev_avg.pu3 * 60000) + (sag_pu3 * sag_time) - (100 * sag_time))/60000.0
                prev_avg.total_sag1 += add_sag1
                prev_avg.total_sag2 += add_sag2
                prev_avg.total_sag3 += add_sag3
                prev_avg.put()
            else:
                # don't have average
                avg_pu1 = ((sag_pu1 * sag_time) + (100 * (60000 - sag_time)))/60000.0
                avg_pu2 = ((sag_pu2 * sag_time) + (100 * (60000 - sag_time)))/60000.0
                avg_pu3 = ((sag_pu3 * sag_time) + (100 * (60000 - sag_time)))/60000.0
                new_rms_minute = RMSMinute(
                    pu1=avg_pu1,
                    pu2=avg_pu2,
                    pu3=avg_pu3,
                    timestamp=minute_block_from,
                    total_sag1=add_sag1,
                    total_sag2=add_sag2,
                    total_sag3=add_sag3)
                new_rms_minute.put()

            # fill next overlap block
            sag_time_left = sag_time_all - sag_time
            next_block_pointer = minute_block_from + 60000
            while sag_time_left > 0:
                if sag_time_left > 60000:
                    sag_time_this_block = 60000
                else:
                    sag_time_this_block = sag_time_left

                avg_pu1 = ((sag_pu1 * sag_time_this_block) + (100 * (60000 - sag_time_this_block)))/60000.0
                avg_pu2 = ((sag_pu2 * sag_time_this_block) + (100 * (60000 - sag_time_this_block)))/60000.0
                avg_pu3 = ((sag_pu3 * sag_time_this_block) + (100 * (60000 - sag_time_this_block)))/60000.0
                new_rms_minute = RMSMinute(
                    pu1=avg_pu1,
                    pu2=avg_pu2,
                    pu3=avg_pu3,
                    timestamp=next_block_pointer,
                    total_sag1=0,
                    total_sag2=0,
                    total_sag3=0)
                new_rms_minute.put()

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
        avg_pu1, avg_pu2, avg_pu3, sag_pu1, sag_pu2, sag_pu3 = _avg_rms_list(rms_list, 60)
        rms_query = ToTable.query_rms(block_pointer,
                                         block_pointer + block_time - 1, None, False)
        if rms_query.count() > 0:
            # already have the average, use previous average to calculate next avg
            prev_avg = rms_query.get()
            prev_avg.pu1 = avg_pu1
            prev_avg.pu2 = avg_pu2
            prev_avg.pu3 = avg_pu3
            prev_avg.total_sag1 = sag_pu1
            prev_avg.total_sag2 = sag_pu2
            prev_avg.total_sag3 = sag_pu3
            prev_avg.put()
        else:
            # don't have average
            new_rms = ToTable(
                pu1=avg_pu1,
                pu2=avg_pu2,
                pu3=avg_pu3,
                timestamp=block_pointer,
                total_sag1=sag_pu1,
                total_sag2=sag_pu2,
                total_sag3=sag_pu3)
            new_rms.put()

        block_pointer += block_time
        rms_list = FromTable.query_rms(block_pointer,
                                        block_pointer + block_time - 1,
                                        None, False).fetch()

def _avg_rms_list(rms_list, n):
    sum_pu1 = 0
    sum_pu2 = 0
    sum_pu3 = 0
    sag_pu1 = 0
    sag_pu2 = 0
    sag_pu3 = 0
    length = len(rms_list)
    for i in range(0, n):
        if i < length:
            sum_pu1 += rms_list[i].pu1
            sum_pu2 += rms_list[i].pu2
            sum_pu3 += rms_list[i].pu3
            sag_pu1 += rms_list[i].total_sag1
            sag_pu2 += rms_list[i].total_sag2
            sag_pu3 += rms_list[i].total_sag3
        else:
            sum_pu1 += 100
            sum_pu2 += 100
            sum_pu3 += 100
    avg_pu1 = sum_pu1 / float(n)
    avg_pu2 = sum_pu2 / float(n)
    avg_pu3 = sum_pu3 / float(n)

    return avg_pu1, avg_pu2, avg_pu3, sag_pu1, sag_pu2, sag_pu3


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
        if start is None:
            start = RMS.query_rms(None, None, 1, True)[0].timestamp
        if end is None:
            end = RMS.query_rms(None, None, 1, False)[0].timestamp

        selected_range = end - start

        if selected_range < 30 * 60 * 60 * 1000:               # 30 hours range
            # return 1 point per minute
            query = RMSMinute.query_rms(start, end, count, is_asc)
        elif selected_range < 31 * 24 * 60 * 60 * 1000:          # one month range
            # return 1 point per hour
            query = RMSHour.query_rms(start, end, count, is_asc)
        else:                                                    # > one month range
            # return 1 point per day
            query = RMSDay.query_rms(start, end, count, is_asc)
    else:
        query = RMS.query_rms(start, end, count, is_asc)

    rms_list = []
    for rms in query:
        data = {'timestamp': rms.timestamp, 'pu1': rms.pu1, 'pu2': rms.pu2, 'pu3': rms.pu3}
        if is_scale is True:
            data.update({'total_sag1': rms.total_sag1, 'total_sag2': rms.total_sag2, 'total_sag3': rms.total_sag3})
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
        logging.info('nolist')
    else:
    #if 1==1:
        for tmp_ts in querylastts:
            last_ts_query = tmp_ts.lastts

        data=RMS.query(RMS.timestamp > last_ts_query).order(-RMS.timestamp).fetch()

        users = Email.query().fetch()
        logging.info('len='+str(range(len(data))))
        logging.info(data)
        if data:
            for user in users:
                logging.info(user.email+" "+str(user.enable))
                sag1 = 0
                sag2 = 0
                sag3 = 0
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
                            body_pu1 = 'Phase 1 sag : Voltage changed for %s %%\n' % tmp.pu1
                            body_forsend += body_pu1
                        if tmp.pu2 > user.percent:
                            sag2 += 1
                            body_pu2 = 'Phase 2 sag : Voltage changed for %s %%\n' % tmp.pu2
                            body_forsend += body_pu2
                        if tmp.pu3 > user.percent:
                            sag3 += 1
                            body_pu3 = 'Phase 3 sag : Voltage changed for %s %%\n' % tmp.pu3
                            body_forsend += body_pu3
                        if tmp.pu1 > user.percent or tmp.pu2 > user.percent or tmp.pu3 > user.percent:
                            body_forsend += 'at time ' + str(time_readable) + '\n\n'

                    mail.send_mail(sender='toonja1990@gmail.com',
                                   to=user.email,
                                   subject='Voltage Sag Alert !', body=body_forsend)

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

