import sys
import os
from datetime import datetime,timedelta
from erppeek import Client
import csv
import pytz
import uuid

import dbconfig
import click


tz = pytz.timezone('Europe/Madrid')

class CchPool(object):
    def __init__(self):
        pass

    def get(self, path):
        import csv
        m = []
        with open(path) as csvfile:
            readCSV = csv.reader(csvfile, delimiter=';')
            for row in readCSV:
                cups,dt,daylight,value=row[0:4]
                daylight = int(daylight)
                value = int(value)
                validated = True if int(row[10])==1 else False
                dt = datetime.strptime(dt, '%Y/%m/%d %H:%M')
                m.append({
                    'datetime': dt,
                    'name': cups,
                    'ai': value,
                    'season': daylight,
                    'validated': validated 
                })
        return m 


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
    print "running...."
    ctx.obj['cch'] = CchPool()
    ctx.obj['cups'] = CupsPool(dbconfig.erppeek)
    print "running...."

@uploader.command()
@click.pass_context
@click.argument('path', type=click.Path(exists=True))
@click.argument('_path', type=click.Path(exists=True))
@click.option('--maxfiles', default=100)
def post(ctx, path, _path, maxfiles):
    cch = ctx.obj['cch']
    cups = ctx.obj['cups']

    writer = WriterPool(path, maxfiles)
    for path in os.listdir(_path):
        path = os.path.join(_path, path)
        writer.write(cups, cch.get(path))

if __name__ == '__main__':
    print "started"
    import time
    start_time = time.time()
    uploader(obj=dict())
    print("--- %s seconds ---" % (time.time() - start_time))

# vim: et ts=4 sw=4
