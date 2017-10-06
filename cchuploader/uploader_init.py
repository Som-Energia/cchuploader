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

class CchPool(object):
    def __init__(self, mongo):
        self.mongo = pymongo.MongoClient(mongo['uri'])
        self.cch = self.mongo[mongo['dbname']][mongo['collection']]

    def get(self, start=None, end=None):
        assert start is not None, (
            "Lower bound not defined")
        end = asutc(now()) if not end else end

        filters = dict(
            update_at = {
                '$gt' : datetime.strptime(str(start), "%Y%m"),
                '$lt' : datetime.strptime(str(end), "%Y%m")
            })
        return (self.cch
            .find(filters, ['datetime', 'name', 'ai', 'season', 'validated']))

class CupsPool(object):
    def __init__(self, erp):
        erp = Client(**erp)

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
        def line(ts, ai, validated):
            return ('%s;%d;%s' % (ts,ai,validated)) + '\n'
        for m in miter:
            ct = index.toContract(m['name'])
            if not ct:
                continue
            dt = m['datetime']
            dst = int(m['season'])
            ts = tz.localize(dt, is_dst=dst).astimezone(pytz.utc) 
            ts -= timedelta(hours=1) 
            ai = m['ai']
            validated = m['validated']
            fd = self.allocate(ct)
            fd.write(line(ts,ai,validated))

    def __del__(self):
        for k in self.pool.keys():
            self.pool[k].close()
            del self.pool[k]

@click.group()
@click.pass_context
def uploader(ctx):
    ctx.obj['cch'] = CchPool(dbconfig.mongo)
    ctx.obj['cups'] = CupsPool(dbconfig.erppeek)

@uploader.command()
@click.pass_context
@click.argument('path', type=click.Path(exists=True))
@click.option('--days', default=14)
@click.option('--start_month', default=201410)
@click.option('--end_month', default=201410)
@click.option('--maxfiles', default=100)
def post(ctx, path, days, start_month, end_month, maxfiles):
    cch = ctx.obj['cch']
    cups = ctx.obj['cups']

    writer = WriterPool(path, maxfiles)

    start = start_month
    end = end_month
    writer.write(cups, cch.get(start, end))

if __name__ == '__main__':
    uploader(obj=dict())

# vim: et ts=4 sw=4
