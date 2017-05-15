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

def isodate(date):
    return datetime.strptime(date, '%Y-%m-%d')

class CchPool(object):
    def __init__(self, mongo):
        self.mongo = pymongo.MongoClient(mongo['uri'])
        self.cch = self.mongo[mongo['dbname']][mongo['collection']]

    def get(self, start=None, end=None):
        assert start is not None, (
            "Lower bound not defined")
        end = asUtc(now()) if not end else end

        filters = dict(
            validated = True,
            update_at = {
                '$gt': start,
                '$lt': end
            })
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
        self.index = { c['cups'][1][:20]:c['name']
                for c in ct_obj.read(
                        ct_obj.search(filters), ['name', 'cups'])}

    def isActive(self, cups):
        return cups[:20] in self.index

    def toContract(self, cups):
        return self.index[cups[:20]] if self.isActive(cups) else None

class WriterPool(object):
    def __init__(self, path, maxfiles):
        self.path = path
        self.pool = dict()
        self.maxfiles = maxfiles

    def allocate(self, name):
        if name in self.pool:
            return self.pool[name]

        fds = self.pool.keys()
        if len(fds) > self.maxfiles:
            for i in range(int(0.25*self.maxfiles)):
                self.pool[fds[i]].close()
                del self.pool[fds[i]]
        filename = os.path.join(self.path, name + '.csv')
        self.pool[name] = open(filename, 'a')
        return self.pool[name]

    def write(self, index, miter):
        def line(ts, ai):
            return ('%s;%d' % (ts,ai)) + '\n'

        n = 0
        cts = set()
        for m in miter:
            ct = index.toContract(m['name'])
            if not ct:
                continue
            ts = m['datetime']
            ai = m['ai']

            fd = self.allocate(ct)
            fd.write(line(ts,ai))

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
            limit=1, order='start_date desc')
        start_date = p_obj.read(p_id, ['start_date'])['start_date'] \
            if p_id else '1970-01-01'
        return start_date

    def write(self, start, end, contracts, measurements, status, message):
        p_obj = self.erp.model('empowering.cch.push.log')
        values = {
            'start_date': start,
            'end_date': end,
            'contracts': contracts,
            'measurements': measurements,
            'status': 'failed' if failed else 'done',
            'message': message
        }
        p_obj.create(values)


@click.group()
@click.pass_context
def uploader(ctx):
    ctx.obj['cch'] = CchPool(dbconfig.mongo)

    erp = Client(**dbconfig.erppeek)
    ctx.obj['cups'] = CupsPool(erp)
    ctx.obj['log'] = PushLog(erp)

@uploader.command()
@click.pass_context
@click.argument('path', type=click.Path(exists=True))
@click.option('--maxfiles', default=100)
def post(ctx, path, days, maxfiles):
    cch = ctx.obj['cch']
    cups = ctx.obj['cups']
    log = ctx.obj['log']

    start = isodate(log.get_start())
    end = now()

    start_ = now()
    status = 'done'
    message = ''
    nc = 0 # number of contracts
    nm = 0 # number of measurements
    try:
        writer = WriterPool(path, maxfiles)
        nc,nm = writer.write(cups,
            cch.get(asutc(start), asutc(end)))
    except Exception as e:
        status = 'failed'
        message = str(e)
    end_ = now()

    log.write(start_, end_, nc, nm, status, message)


if __name__ == '__main__':
    uploader(obj=dict())

# vim: et ts=4 sw=4
