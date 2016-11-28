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

def asUtc(ts):
    return ts.astimezone(pytz.utc)

def minus_days(date, days):
    return date - timedelta(days=days)

def make_uuid(model, model_id):
    if isinstance(model, unicode):
        model = model.encode('utf-8')
    if isinstance(model_id, unicode):
        model_id = model_id.encode('utf-8')
    token = '%s,%s' % (model, model_id)
    return str(uuid.uuid5(uuid.NAMESPACE_OID, token))

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

class WriterPool(object):
    def __init__(self, index, path, maxfiles):
        self.index = index
        self.path = path
        self.pool = dict()
        self.maxfiles = maxfiles

    def allocate(self, name):
        if name in self.pool:
            return self.pool[name]

        fds = self.pool.keys()
        if len(fds) > self.maxfiles:
            for i in range(int(0.25*maxfiles)):
                self.pool[fds[i]].close()
                del self.pool[fds[i]]
        filename = os.path.join(self.path, name + '.csv')
        self.pool[name] = open(filename, 'a')
        return self.pool[name]

    def write(self, index, miter):
        def nameit(name):
            return make_uuid('giscedata.cups.ps', name)
        def line(ts, ai):
            return ('%s;%d' % (ts,ai)) + '\n'
        def find(index, record):
            if record in index:
                return record
            if record[:20] in index:
                return record[:20]
            return None

        for m in miter:
            cups = find(index, m['name'])
            if not cups:
                continue
            ts = m['datetime']
            ai = m['ai']

            fd = self.allocate(nameit(cups))
            fd.write(line(ts,ai))

    def __del__(self):
        for k in self.pool.keys():
            self.pool[k].close()
            del self.pool[k]

@click.group()
@click.pass_context
def uploader(ctx):
    ctx.obj['pool'] = CchPool(dbconfig.mongo)
    ctx.obj['erp'] = Client(**dbconfig.erppeek)

@uploader.command()
@click.pass_context
@click.argument('path', type=click.Path(exists=True))
@click.option('--days', default=14)
@click.option('--maxfiles', default=100)
def post(ctx, path, days, maxfiles):
    pool = ctx.obj['pool']
    erp_obj = ctx.obj['erp']
    cups_obj = erp_obj.model('giscedata.cups.ps')

    index = [c['name'] 
        for c in cups_obj.read(
           cups_obj.search([('empowering', '=', True)]), ['name'])]
    writer = WriterPool(index, path, maxfiles)
    end = asUtc(now())
    start = minus_days(end, days)
    writer.write(index, pool.get(start, end))

if __name__ == '__main__':
    uploader(obj=dict())

# vim: et ts=4 sw=4
