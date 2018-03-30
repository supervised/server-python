import time
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
import urlparse
import glob
import os
import os.path
import dateutil.parser


class StdoutFeedback():
    def __init__(self):
        print 'feedback is over stdout'
        
    def setup(self):    
        print 'setup feedback.'

    def destroy(self):
        print 'destroy feedback.'

    def start(self,requestHeaders):
        print '----------'
        print requestHeaders
        
    def finish(self,responseHeaders):
        print '---'
        for h in responseHeaders:
            print '%s: %s' % ( h, responseHeaders[h] )
        print '----------'
    
class GpioFeedback():
    def __init__(self,GPIO,ledpin):
        print 'feedback is over GPIO pin ',ledpin
        self.ledpin=ledpin
        self.GPIO=GPIO
    def setup(self):    
        self.GPIO.setwarnings(False)
        #set the gpio modes to BCM numbering
        self.GPIO.setmode(GPIO.BCM)
        #set LEDPIN's mode to output,and initial level to LOW(0V)
        self.GPIO.setup(LEDPIN,GPIO.OUT,initial=GPIO.LOW)
        self.GPIO.output(LEDPIN,GPIO.HIGH)
        time.sleep(0.2)
        self.GPIO.output(LEDPIN,GPIO.LOW)

    def destroy(self):
        #turn off LED
        self.GPIO.output(LEDPIN,GPIO.LOW)
        #release resource
        self.GPIO.cleanup()
    def start(self,requestHeaders):
        self.GPIO.output(LEDPIN,GPIO.HIGH)
    def finish(self,responseHeaders):
        self.GPIO.output(LEDPIN,GPIO.LOW)
        
#import RPi.GPIO as GPIO
#feedback= GpioFeedback(RPi.GPIO,27)  # When this is installed on the Raspberry PI
feedback= StdoutFeedback()  # When testing at the unix command line.

#HOST_NAME = '192.168.0.18' # !!!REMEMBER TO CHANGE THIS!!!
HOST_NAME = '192.168.0.205' # !!!REMEMBER TO CHANGE THIS!!!
PORT_NUMBER = 9000 # Maybe set this to 9000.
HAPI_HOME= '/home/jbf/hapi/'

# Configuration requirements
# * capabilities and catalog responses must be formatted as JSON in SERVER_HOME.
# * info responses are in SERVER_HOME/info.
# * responses can have templates like "lasthour" to mean the last hour boundary, and "lastday-P1D" to mean the last midnight minus one day.
# * data files must be in daily csv files, SERVER_HOME/data/<id>/$Y/<id>.$Y$m$d.csv
# * each data file must have time as $Y-$m-$dT$H:$M:$SZ
# * embedded info response, preceding the CSV response, is not yet supported.

def do_write_info( s, id, parameters, prefix ):
    try:
        infoJson= open( HAPI_HOME + 'info/' + id + '.json' ).read()
        import json
        infoJsonModel= json.loads(infoJson)
        if ( parameters!=None ):
            allParameters= infoJsonModel['parameters']
            newParameters= []
            includeParams= set(parameters)
            for i in xrange(len(allParameters)):
                if ( i==0 or allParameters[i]['name'] in includeParams ):
                    newParameters.append( allParameters[i] )
            infoJsonModel['parameters']= newParameters
        infoJson= json.dumps( infoJsonModel, indent=4, separators=(',', ': '))
        for l in infoJson.split('\n'):
            l= do_info_macros(l)
            if ( prefix!=None ): s.wfile.write(prefix)
            s.wfile.write(l)
            s.wfile.write('\n');
    except:
        send_exception(s.wfile,'Not Found') 

def get_last_modified( id, timemin, timemax ):
    'return the time stamp of the most recently modified file, from files in $Y/$(x,name=id).$Y$m$d.csv'
    ff= HAPI_HOME + 'data/' + id + '/'
    filemin= dateutil.parser.parse( timemin ).strftime('%Y%m%d')
    filemax= dateutil.parser.parse( timemax ).strftime('%Y%m%d')
    timemin= dateutil.parser.parse( timemin ).strftime('%Y-%m-%dT%H:%M:%S')
    timemax= dateutil.parser.parse( timemax ).strftime('%Y-%m-%dT%H:%M:%S')
    yrmin= int( timemin[0:4] )
    yrmax= int( timemax[0:4] )
    lastModified= None
    from email.utils import formatdate
    for yr in range(yrmin,yrmax+1):
        ffyr= ff + '%04d' % yr
        if ( not os.path.exists(ffyr) ): continue
        files= sorted( os.listdir( ffyr ) ) 
        for file in files:
             ymd= file[-12:-4]
             if ( filemin<=ymd and ymd<=filemax ):
                  mtime= os.path.getmtime( ffyr + '/' + file )
                  if ( lastModified==None or mtime>lastModified ): lastModified=mtime
                  #print 'line87: ', file, formatdate( timeval=mtime, localtime=False, usegmt=True )
    #print 'line89: ', formatdate( timeval=lastModified, localtime=False, usegmt=True )
    return lastModified
	
def do_data_csv( id, timemin, timemax, parameters, s ):
    ff= HAPI_HOME + 'data/' + id + '/'
    filemin= dateutil.parser.parse( timemin ).strftime('%Y%m%d')
    filemax= dateutil.parser.parse( timemax ).strftime('%Y%m%d')
    timemin= dateutil.parser.parse( timemin ).strftime('%Y-%m-%dT%H:%M:%S')
    timemax= dateutil.parser.parse( timemax ).strftime('%Y-%m-%dT%H:%M:%S')
    yrmin= int( timemin[0:4] )
    yrmax= int( timemax[0:4] )
    if ( parameters!=None ):
        mm= do_parameters_map( id, parameters )
    else:
        mm= None
    for yr in range(yrmin,yrmax+1):
        ffyr= ff + '%04d' % yr
        if ( not os.path.exists(ffyr) ): continue
        files= sorted( os.listdir( ffyr ) ) 
        for file in files:
             ymd= file[-12:-4]
             if ( filemin<=ymd and ymd<=filemax ):
                  for rec in open( ffyr + '/' + file ):
                      ydmhms= rec[0:19]
                      if ( timemin<=ydmhms and ydmhms<timemax ):
                          if ( mm!=None ):
                              ss= rec.split(',')
                              comma= False
                              for i in mm:
                                 if comma: 
                                    s.wfile.write(',')
                                 s.wfile.write(ss[i])
                                 comma=True
                              if mm[-1]<(len(ss)-1):
                                 s.wfile.write('\n')
                          else:
                              s.wfile.write(rec)

def do_info_macros( line ):
    ss= line.split('"now"')
    if ( len(ss)==2 ):
       import time
       return ss[0] + '"' + time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())+ '"' + ss[1]
    ss= line.split('"lastday-P1D"')
    if ( len(ss)==2 ):
       from datetime import datetime, date, time
       midnight = datetime.combine(date.today(), time.min)
       from datetime import timedelta
       yesterday_midnight = midnight - timedelta(days=1)
       return ss[0] + '"' + yesterday_midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+ '"' + ss[1]
    ss= line.split('"lastday"')
    if ( len(ss)==2 ):
       from datetime import datetime, date, time
       midnight = datetime.combine(date.today(), time.min) # TODO: bug lastday is probably based on local time.
       return ss[0] + '"' + midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+ '"' + ss[1]
    ss= line.split('"lasthour"')
    if ( len(ss)==2 ):
       from datetime import datetime, date, time
       midnight = datetime.combine(date.today(), time.min)  # TODO: bug lasthour is implemented as lastday
       return ss[0] + '"' + midnight.strftime('%Y-%m-%dT%H:%M:%SZ')+ '"' + ss[1]
    return line
    
def send_exception( w, msg ):
    w.write( '{ "HAPI": "2.0", "status": { "code": 1406, "message": "%s" } }\n' % msg )

def do_get_parameters( id ):
    if ( id=='10.CF3744000800' ):
        return [ 'Time','Temperature' ]
    elif ( id=='cputemp' ):
        return [ 'Time', 'GPUTemperature', 'CPUTemperature' ]
    else:
        raise Except("this is not implemented!")

def handle_key_parameters( query ):
    'return the parameters in an array, or None'
    if query.has_key('parameters'):
        parameters= query['parameters'][0] 
        parameters= parameters.split(',')
    else:
        parameters= None
    return parameters

def do_parameters_map( id, parameters ):
    pp= do_get_parameters(id)
    result= map( pp.index, parameters )
    if ( result[0]!=0 ):
        result.insert(0,0)
    return result

def get_forwarded(headers):
    'This doesn''t work...'
    #for h in headers: print h, '=', headers.get(h)
    if headers.has_key('x-forwarded-server'):
        return headers.get('x-forwarded-server')
    else:
        return None 


class MyHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_HEAD(s):
        s.send_response(200)
        s.send_header("Content-type", "application/json")
        s.end_headers()

    def do_GET(s):
        
        feedback.start(s.headers)
        responseHeaders= {}
        
        path= s.path
        pp= urlparse.urlparse(s.path)
        
        while ( path.endswith('/') ):
            path= path[:-1]

        i= path.find('?')
        if ( i>-1 ): path= path[0:i] 

        while ( path.startswith('/') ):
            path= path[1:]

        query= urlparse.parse_qs( pp.query )

        if ( path=='hapi/capabilities' ):                
           s.send_response(200)
           s.send_header("Content-type", "application/json")

        elif ( path=='hapi/catalog' ):
           s.send_response(200)
           s.send_header("Content-type", "application/json")

        elif ( path=='hapi/info' ):
           id= query['id'][0]
           if ( os.path.isfile(HAPI_HOME + 'info/' + id + '.json' ) ):
               s.send_response(200)
           else:
               s.send_response(404)
           s.send_header("Content-type", "application/json")

        elif ( path=='hapi/data' ):
           id= query['id'][0]
           timemin= query['time.min'][0]
           timemax= query['time.max'][0]
           lastModified= get_last_modified( id, timemin, timemax );
           # check request header for If-Modified-Since
           if ( os.path.isfile(HAPI_HOME + 'info/' + id + '.json' ) ):
               s.send_response(200)
               s.send_header("Content-type", "text/csv")
           else:
               s.send_response(404)
           s.send_header("Content-type", "text/csv")

        elif ( path=='hapi' ):
           s.send_response(200)
           s.send_header("Content-type", "text/html")

        else:
           s.send_response(404)
           s.send_header("Content-type", "application/json")

        s.send_header("Access-Control-Allow-Origin", "*")
        s.send_header("Access-Control-Allow-Methods", "GET")
        s.send_header("Access-Control-Allow-Headers", "Content-Type")

        if ( path=='hapi/data' ):
            from email.utils import formatdate
            responseHeaders['Last-Modified']= formatdate( timeval=lastModified, localtime=False, usegmt=True ) 
        
        for h in responseHeaders:
            s.send_header(h,responseHeaders[h])
            
        s.end_headers()

        if ( path=='hapi/capabilities' ):
            for l in open( HAPI_HOME + 'capabilities.json' ):
                s.wfile.write(l)
        elif ( path=='hapi/catalog' ):
            for l in open( HAPI_HOME + 'catalog.json' ):
                s.wfile.write(l)
        elif ( path=='hapi/info' ):
            id= query['id'][0]
            parameters= handle_key_parameters(query)
            do_write_info( s, id, parameters, None )
        elif ( path=='hapi/data' ):
            id= query['id'][0]
            timemin= query['time.min'][0]
            timemax= query['time.max'][0]
            parameters= handle_key_parameters(query)
            if query.has_key('include'):
                if query['include'][0]=='header':
                    do_write_info( s, id, parameters, '#' )
            do_data_csv( id, timemin, timemax, parameters, s )
        elif ( path=='hapi' ):
            s.wfile.write("<html><head><title>Python HAPI Server</title></head>\n")
            s.wfile.write("<body><p>This is a simple Python-based HAPI server, which can be run on a Raspberry PI.</p>\n")
            s.wfile.write("<p>Example requests:</p>\n")
            u= "hapi/catalog" 
            s.wfile.write("<a href='%s'>%s</a></br>\n" % ( u,u ) )
            ff= glob.glob( HAPI_HOME + 'info/*.json' )
            n= len( HAPI_HOME + 'info/' )
            timemin= '2018-01-19T00:00Z'
            timemax= '2018-01-20T00:00Z'
            for f in ff:
                u= "hapi/info?id=%s" % ( f[n:-5] )
                s.wfile.write("<a href='%s'>%s</a></br>\n" % ( u,u ) )
                u= "hapi/data?id=%s&time.min=%s&time.max=%s" % ( f[n:-5], timemin, timemax )
                s.wfile.write("<a href='%s'>%s</a></br>\n" % ( u,u ) )
            s.wfile.write("<br><a href='hapi/data?id=cputemp&time.min=2018-01-19T00:00Z&time.max=2018-01-20T00:00Z&parameters=Time,CPUTemperature'>subset of parameters</a>\n" )
            s.wfile.write("<br><a href='hapi/data?id=cputemp&time.min=2018-01-19T00:00Z&time.max=2018-01-20T00:00Z&include=header&parameters=Time,CPUTemperature'>withInclude</a>\n"  )
            s.wfile.write("<br><a href='hapi/info?id=cputemp&include=header&parameters=Time,CPUTemperature'>infoSubset</a>"  )
            s.wfile.write("<br><br><a href='http://192.168.0.46:2121/'>1-wire http</a>\n")
            s.wfile.write("</body></html>\n")
        else:
            for l in open( HAPI_HOME + 'error.json' ):
                s.wfile.write(l)
        feedback.finish(responseHeaders)

class ThreadedHTTPServer( ThreadingMixIn, HTTPServer ):
   '''Handle requests in a separate thread.'''

if __name__ == '__main__':
    feedback.setup()

    httpd = ThreadedHTTPServer((HOST_NAME, PORT_NUMBER), MyHandler)
    print time.asctime(), "Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    feedback.destroy()

    httpd.server_close()
    print time.asctime(), "Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER)

