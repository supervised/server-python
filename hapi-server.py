import time
import BaseHTTPServer
import urlparse
import glob
import os
import dateutil.parser

HOST_NAME = '192.168.0.205' # !!!REMEMBER TO CHANGE THIS!!!
PORT_NUMBER = 9000 # Maybe set this to 9000.

HAPI_HOME= '/home/jbf/hapi_python/'
SERVER_HOME= '/hapi/'

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
    

class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_HEAD(s):
        s.send_response(200)
        s.send_header("Content-type", "application/json")
        s.end_headers()
    def do_GET(s):
        pp= urlparse.urlparse(s.path)
        if ( pp.path.endswith('capabilities') ):                
           s.send_response(200)
           s.send_header("Content-type", "application/json")
        elif ( pp.path.endswith('catalog') ):
           s.send_response(200)
           s.send_header("Content-type", "application/json")
        elif ( pp.path.endswith('info') ):
           s.send_response(200)
           s.send_header("Content-type", "application/json")
        else:
           s.send_response(200)
           s.send_header("Content-type", "text/html")
        s.end_headers()

        if ( pp.path.startswith(SERVER_HOME) ):
            path= pp.path[len(SERVER_HOME):]
        else:
            path= pp.path

        if ( path=='capabilities' ):
            for l in open( HAPI_HOME + 'capabilities.json' ):
                s.wfile.write(l)
        elif ( path=='catalog' ):
            for l in open( HAPI_HOME + 'catalog.json' ):
                s.wfile.write(l)
        elif ( path=='info' ):
            query= urlparse.parse_qs( pp.query )
            id= query['id'][0]
            for l in open( HAPI_HOME + 'info/' + id + '.json' ):
                l= do_info_macros(l)
                s.wfile.write(l)
        elif ( path=='data' ):
            query= urlparse.parse_qs( pp.query )
            id= query['id'][0]
            timemin= query['time.min'][0]
            timemax= query['time.max'][0]
            do_data_csv( id, timemin, timemax, None, s )
        else:
            s.wfile.write("<html><head><title>Python HAPI Server</title></head>")
            s.wfile.write("<body><p>This is a simple Python-based HAPI server, to be run on a Raspberry PI.</p>")
            s.wfile.write("<p>You accessed path: %s</p>" % s.path)
            s.wfile.write("</body></html>")

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
