import os
from datetime import datetime,timedelta
from erppeek import Client 
import pymongo
import pytz
import uuid

import dbconfig
import click


tz = pytz.timezone('Europe/Madrid')

def now():
    return tz.localize(datetime.now(), is_dst=None)

def asutc(ts):
    return ts.astimezone(pytz.utc)

def isodatetime(date):
    return tz.localize(datetime.strptime(date, '%Y-%m-%d %H:%M:%S'))

def isodate(date):
    return tz.localize(datetime.strptime(date, '%Y-%m-%d'))

class CchPool(object):
    def __init__(self, mongo):
        self.mongo = pymongo.MongoClient(mongo['uri'])
        self.cch = self.mongo[mongo['dbname']][mongo['collection']]
        self.mongo_collection = mongo['collection']

    def get(self, start=None, end=None):
        assert start is not None, (
            "Lower bound not defined")
        end = asUtc(now()) if not end else end

        filters = dict(
            create_at = {
                '$gt': start,
                '$lt': end
            })
        if self.mongo_collection == 'tg_cchfact':
            return (self.cch
                .find(filters, ['datetime', 'name', 'ai', 'season', 'validated']))
        else:
            return (self.cch
                .find(filters, ['datetime', 'name', 'ai']))

class CupsPool(object):
    def __init__(self, erp):
        ct_obj = erp.model('giscedata.polissa')
        filters = [
                ('cups.empowering', '=', True),
                ('state', '=', 'activa'),
                ('active', '=', True),
                ]
        fields_to_read = ['name', 'cups', 'data_alta']
        self.index = {}
        self.index_datestart = {}
        for c in ct_obj.read(ct_obj.search(filters), fields_to_read):
            self.index[c['cups'][1][:20]] = c['name']
            self.index_datestart[c['name']] = asutc(isodate(c['data_alta']))

    def isActive(self, cups, ts):
        name = cups[:20]
        return (name in self.index and
            (ts >= self.index_datestart[self.index[name]]))

    def toContract(self, cups, ts):
        return self.index[cups[:20]] if self.isActive(cups, ts) else None

class WriterPool(object):
    def __init__(self, path, maxfiles):
        self.path = path
        self.pool = dict()
        self.maxfiles = maxfiles

    def allocate(self, name, cch_type):
        if name in self.pool:
            return self.pool[name]

        fds = self.pool.keys()
        if len(fds) > self.maxfiles:
            for i in range(int(0.25*self.maxfiles)):
                self.pool[fds[i]].close()
                del self.pool[fds[i]]
        filename = os.path.join(self.path, name + '_' + cch_type + '.csv')
        self.pool[name] = open(filename, 'a')
        return self.pool[name]

    def write(self, index, miter, cch_type):
        def line_p5d(ts, ai):
            return ('%s;%d' % (ts,ai)) + '\n'

        def line(ts, ai, validated):
            return ('%s;%d;%s' % (ts,ai,validated)) + '\n'
        n = 0
        cts = set()
        for m in miter:
            if  cch_type == 'tg_cchval':
                dt = m['datetime']
                ts = tz.localize(dt).astimezone(pytz.utc)
                ct = index.toContract(m['name'], ts)
                if not ct:
                    continue
                ts -= timedelta(hours=1)
                ai = m['ai']
                fd = self.allocate(ct, cch_type)
                fd.write(line_p5d(ts, ai))
            else:
                dt = m['datetime']
                dst = int(m['season'])
                ts = tz.localize(dt, is_dst=dst).astimezone(pytz.utc)
                ct = index.toContract(m['name'], ts)
                if not ct:
                    continue
                ts -= timedelta(hours=1)
                ai = m['ai']
                validated = m['validated']
                fd = self.allocate(ct, cch_type)
                fd.write(line(ts,ai,validated))

            n += 1
            cts.add(ct)
        return(len(cts), n)

    def __del__(self):
        for k in self.pool.keys():
            self.pool[k].close()
            del self.pool[k]

class PushLog(object):
    def __init__(self, erp):
        self.erp = erp

    def get_start(self):
        p_obj = self.erp.model('empowering.cch.push.log')
        p_id = p_obj.search([('status', '=', 'done')],
            limit=1, order='start_date desc')[0]
        start_date = p_obj.read(p_id, ['start_date'])['start_date'] \
            if p_id else '1970-01-01'
        return start_date

    def write(self, start, end, contracts, measurements, status, message):
        start = start.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
        end = end.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
        p_obj = self.erp.model('empowering.cch.push.log')
        values = {
            'start_date': start,
            'end_date': end,
            'contracts': contracts,
            'measurements': measurements,
            'status': status,
            'message': message
        }
        p_obj.create(values)


@click.group()
@click.pass_context
@click.option(
    '--curve',
    default='cchfact',
    help='Name of the curve collection. cchfact, cchval...'
)
def uploader(ctx, curve):
    conn_data = getattr(dbconfig, 'mongo_{}'.format(curve), '')
    if not conn_data:
        msg = "Not mongo configuration found for curve {}"
        raise click.ClickException(msg.format(curve))

    ctx.obj['cch'] = CchPool(conn_data)
    ctx.obj['cch_type'] = conn_data['collection']

    erp = Client(**dbconfig.erppeek)
    ctx.obj['cups'] = CupsPool(erp)
    ctx.obj['log'] = PushLog(erp)


@uploader.command()
@click.pass_context
@click.argument('path', type=click.Path(exists=True))
@click.option('--maxfiles', default=100)
def post(ctx, path, maxfiles):
    cch = ctx.obj['cch']
    cups = ctx.obj['cups']
    log = ctx.obj['log']
    cch_type = ctx.obj['cch_type']

    start = isodatetime(log.get_start())
    end = now()

    start_ = now()
    status = 'done'
    message = ''
    nc = 0 # number of contracts
    nm = 0 # number of measurements
    try:
        writer = WriterPool(path, maxfiles)
        nc,nm = writer.write(cups,
            cch.get(asutc(start), asutc(end)),
            cch_type)
    except Exception as e:
        print e
        status = 'failed'
        message = str(e)
    end_ = now()

    log.write(start_, end_, nc, nm, status, message)


if __name__ == '__main__':
    uploader(obj=dict())

# vim: et ts=4 sw=4
