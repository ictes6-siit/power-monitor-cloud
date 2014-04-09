"""`main` is the top level module for your Flask application."""

# Import the Flask Framework
from google.appengine.ext import ndb
from flask import Flask,jsonify
from flask import request
from crossdomain import crossdomain
from model import *
app = Flask(__name__)

@app.route('/test', methods=['GET'])
def get_test():
    ndb.delete_multi(RMSSecond.query().iter(keys_only = True))
    return jsonify({'status': 'OK'}), 200

@app.route('/rms.json', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*', headers='Origin, X-Requested-With, Content-Type, Accept')
def post_rms():
    this_rms = RMS(
        pu1=request.json['pu1'],
        pu2=request.json['pu2'],
        pu3=request.json['pu3'],
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
            sag_time_can = (prev_rms.timestamp - (prev_rms.timestamp % 1000)) + 1000 - prev_rms.timestamp
            if sag_time_all < sag_time_can:
                sag_time = sag_time_all
            else:
                sag_time = sag_time_can
            second_block_from = prev_rms.timestamp - (prev_rms.timestamp % 1000)
            second_block_to = second_block_from + 1000
            rms_second_query = RMSSecond.query_rms(second_block_from, second_block_to, None, False)
            if rms_second_query.count() > 0:
                # already have the average, use previous average to calculate next avg
                prev_avg = rms_second_query.get()
                # calculate next avg
                prev_avg.pu1 = ((prev_avg.pu1 * 1000) + (sag_pu1 * sag_time) - (100 * sag_time))/1000.0
                prev_avg.pu2 = ((prev_avg.pu2 * 1000) + (sag_pu2 * sag_time) - (100 * sag_time))/1000.0
                prev_avg.pu3 = ((prev_avg.pu3 * 1000) + (sag_pu3 * sag_time) - (100 * sag_time))/1000.0
                prev_avg.total_sag1 += add_sag1
                prev_avg.total_sag2 += add_sag2
                prev_avg.total_sag3 += add_sag3
                prev_avg.put()
            else:
                # don't have average
                avg_pu1 = ((sag_pu1 * sag_time) + (100 * (1000 - sag_time)))/1000.0
                avg_pu2 = ((sag_pu2 * sag_time) + (100 * (1000 - sag_time)))/1000.0
                avg_pu3 = ((sag_pu3 * sag_time) + (100 * (1000 - sag_time)))/1000.0
                new_rms_minute = RMSSecond(
                    pu1=avg_pu1,
                    pu2=avg_pu2,
                    pu3=avg_pu3,
                    timestamp=second_block_from,
                    total_sag1=add_sag1,
                    total_sag2=add_sag2,
                    total_sag3=add_sag3)
                new_rms_minute.put()

            # fill next overlap block
            sag_time_left = sag_time_all - sag_time
            next_block_pointer = second_block_from + 1000
            while sag_time_left > 0:
                if sag_time_left > 1000:
                    sag_time_this_block = 1000
                else:
                    sag_time_this_block = sag_time_left

                avg_pu1 = ((sag_pu1 * sag_time_this_block) + (100 * (1000 - sag_time_this_block)))/1000.0
                avg_pu2 = ((sag_pu2 * sag_time_this_block) + (100 * (1000 - sag_time_this_block)))/1000.0
                avg_pu3 = ((sag_pu3 * sag_time_this_block) + (100 * (1000 - sag_time_this_block)))/1000.0
                new_rms_second = RMSSecond(
                    pu1=avg_pu1,
                    pu2=avg_pu2,
                    pu3=avg_pu3,
                    timestamp=next_block_pointer,
                    total_sag1=add_sag1,
                    total_sag2=add_sag2,
                    total_sag3=add_sag3)
                new_rms_second.put()

                sag_time_left -= sag_time_this_block
                next_block_pointer += 1000

            # update minute resolution
            _update_resolution(prev_rms, this_rms, (60*1000), RMSSecond, RMSMinute)

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

        if selected_range < 60 * 1000:                             # one minute range
            # return 1 point per second
            query = RMSSecond.query_rms(start, end, count, is_asc)
        elif selected_range < 60 * 60 * 1000:                      # one hour range
            # return 1 point per minute
            query = RMSMinute.query_rms(start, end, count, is_asc)
        elif selected_range < 24 * 60 * 60 * 1000:               # one day range
            # return 1 point per hour
            query = RMSHour.query_rms(start, end, count, is_asc)
        elif selected_range < 31 * 24 * 60 * 60 * 1000:          # one month range
            # return 1 point per day
            query = RMSDay.query_rms(start, end, count, is_asc)
        elif selected_range < 15 * 31 * 24 * 60 * 60 * 1000:     # one year range
            # return 1 point per month
            query = RMSMonth.query_rms(start, end, count, is_asc)
        else:
            query = RMSYear.query_rms(start, end, count, is_asc)
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

