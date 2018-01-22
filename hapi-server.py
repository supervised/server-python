import time
import BaseHTTPServer
import urlparse
import glob
import os
import dateutil.parser
import subprocess

HOST_NAME = '192.168.0.46' # !!!REMEMBER TO CHANGE THIS!!!
#HOST_NAME = '192.168.0.205' # !!!REMEMBER TO CHANGE THIS!!!
PORT_NUMBER = 9000 # Maybe set this to 9000.

HAPI_HOME= '/home/jbf/hapi/'
SERVER_HOME= '/hapi'  # Note no slash

# Configuration requirements
# * capabilities and catalog responses must be formatted as JSON in SERVER_HOME.
# * data files must be in daily csv files, SERVER_HOME/data/<id>/$Y/<id>.$Y$m$d.csv
# * each data file must have time as $Y-$m-$dT$H:$M:$D

def do_data_csv( id, timemin, timemax, parameters, s ):
    ff= HAPI_HOME + 'data/' + id + '/'
    filemin= dateutil.parser.parse( timemin ).strftime('%Y%m%d')
    filemax= dateutil.parser.parse( timemax ).strftime('%Y%m%d')
    timemin= dateutil.parser.parse( timemin ).strftime('%Y-%m-%dT%H:%M:%S')
    timemax= dateutil.parser.parse( timemax ).strftime('%Y-%m-%dT%H:%M:%S')
    yrmin= int( timemin[0:4] )
    yrmax= int( timemax[0:4] )
    for yr in range(yrmin,yrmax+1):
        ffyr= ff + '%04d' % yr
        ymdmin= timemin[0:8]
        ymdmax= timemax[0:8]
        if ( not os.path.exists(ffyr) ): continue
        files= sorted( os.listdir( ffyr ) ) 
        for file in files:
             ymd= file[-12:-4]
             if ( filemin<=ymd and ymd<=filemax ):
                  for rec in open( ffyr + '/' + file ):
                      ydmhms= rec[0:19]
                      if ( timemin<=ydmhms and ydmhms<timemax ):
                          s.wfile.write(rec)

def do_info_macros( line ):
    ss= line.split('"now"')
    if ( len(ss)==2 ):
       import time
       return ss[0] + '"' + time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime())+ '"' + ss[1]
    ss= line.split('"lastday-P1D"')
    if ( len(ss)==2 ):
       from datetime import datetime, date, time
       midnight = datetime.combine(date.today(), time.min)
       from datetime import timedelta
       yesterday_midnight = midnight - timedelta(days=1)
       return ss[0] + '"' + yesterday_midnight.strftime('%Y-%m-%dT%H:%M:%S')+ '"' + ss[1]
    ss= line.split('"lastday"')
    if ( len(ss)==2 ):
       from datetime import datetime, date, time
       midnight = datetime.combine(date.today(), time.min) # TODO: bug lastday is probably based on local time.
       return ss[0] + '"' + midnight.strftime('%Y-%m-%dT%H:%M:%S')+ '"' + ss[1]
    ss= line.split('"lasthour"')
    if ( len(ss)==2 ):
       from datetime import datetime, date, time
       midnight = datetime.combine(date.today(), time.min)  # TODO: bug lasthour is implemented as lastday
       return ss[0] + '"' + midnight.strftime('%Y-%m-%dT%H:%M:%S')+ '"' + ss[1]
    return line
    
def sendException( w, msg ):
    w.write( '{ "HAPI": "2.0", "status": { "code": 1406, "message": "error" } }' )

class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_HEAD(s):
        s.send_response(200)
        s.send_header("Content-type", "application/json")
        s.end_headers()
    def do_GET(s):
        pp= urlparse.urlparse(s.path)

        path= pp.path

        while ( path.endswith('/') ):
            path= path[:-1]

        if ( path.startswith(SERVER_HOME) ):
            path= path[len(SERVER_HOME):]

        while ( path.startswith('/') ):
            path= path[1:]

        print 'path=', path 

        if ( path=='capabilities' ):                
           s.send_response(200)
           s.send_header("Content-type", "application/json")
        elif ( path=='catalog' ):
           s.send_response(200)
           s.send_header("Content-type", "application/json")
        elif ( path=='info' ):
           s.send_response(200)
           s.send_header("Content-type", "application/json")
        elif ( path=='data' ):
           s.send_response(200)
           s.send_header("Content-type", "text/csv")
        elif ( path=='' ):
           s.send_response(200)
           s.send_header("Content-type", "text/html")
        else:
           s.send_response(404)
           s.send_header("Content-type", "application/json")

        s.end_headers()

        if ( path=='capabilities' ):
            for l in open( HAPI_HOME + 'capabilities.json' ):
                s.wfile.write(l)
        elif ( path=='catalog' ):
            for l in open( HAPI_HOME + 'catalog.json' ):
                s.wfile.write(l)
        elif ( path=='info' ):
            query= urlparse.parse_qs( pp.query )
            id= query['id'][0]
            try:
                for l in open( HAPI_HOME + 'info/' + id + '.json' ):
                    l= do_info_macros(l)
                    s.wfile.write(l)
            except:
                sendException(s.wfile,'unable to find '+id) 
        elif ( path=='data' ):
            query= urlparse.parse_qs( pp.query )
            id= query['id'][0]
            timemin= query['time.min'][0]
            timemax= query['time.max'][0]
            do_data_csv( id, timemin, timemax, None, s )
        elif ( path=='' ):
            s.wfile.write("<html><head><title>Python HAPI Server</title></head>")
            s.wfile.write("<body><p>This is a simple Python-based HAPI server, which can be run on a Raspberry PI.</p>")
            s.wfile.write("<p>Example requests:</p>")
            u= "%s://%s:%d/hapi/catalog" % ( 'http', HOST_NAME, PORT_NUMBER )
            s.wfile.write("<a href='%s'>%s</a></br>" % ( u,u ) )
            ff= glob.glob( HAPI_HOME + 'info/*.json' )
            n= len( HAPI_HOME + 'info/' )
            timemin= '2018-01-19T00:00Z'
            timemax= '2018-01-20T00:00Z'
            for f in ff:
                u= "%s://%s:%d/hapi/info?id=%s" % ( 'http', HOST_NAME, PORT_NUMBER, f[n:-5] )
                s.wfile.write("<a href='%s'>%s</a></br>" % ( u,u ) )
                u= "%s://%s:%d/hapi/data?id=%s&time.min=%s&time.max=%s" % ( 'http', HOST_NAME, PORT_NUMBER, f[n:-5], timemin, timemax )
                s.wfile.write("<a href='%s'>%s</a></br>" % ( u,u ) )
            s.wfile.write("</body></html>")
        else:
            for l in open( HAPI_HOME + 'error.json' ):
                s.wfile.write(l)


if __name__ == '__main__':
    server_class = BaseHTTPServer.HTTPServer
    httpd = server_class((HOST_NAME, PORT_NUMBER), MyHandler)
    print time.asctime(), "Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print time.asctime(), "Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER)

