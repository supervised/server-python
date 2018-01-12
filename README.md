# server-python
simple server written in Python, to support Raspberry Pi

# Introduction 
This server is intended to be used when Apache with CGI-BIN scripts would be overkill for the simple task of implementing the HAPI interface.  For example, a Raspberry PI (https://www.raspberrypi.org/) collects temperature sensor data every minute and logs the data to files, and this code implements a HAPI server using only Python.  

# Setup 
Python 2.7 provides a web server in its BaseHTTPServer module.  This is started, and the BaseHTTPServer.BaseHTTPRequestHandler do_GET method handles the different request types (catalog, info, data, etc).  The single Python file "hapi-server.py" is run to start the server.  

The file must be modified for the particular installation.  Four configuration variables are found at the top of the file.  Suppose we wish to run the server at "http://192.168.0.46:9000/hapi/":
* HOST_NAME which is the host name of the device.  (192.168.0.46)
* PORT_NUMBER which is the port for the server.  80 is used for http, or another port, such as 9000, can be used.  
* HAPI_HOME is the folder which will contain the data and templates for the server responses.  (/home/pi/hapi/)
* SERVER_HOME is the folder within the server URLs.  (hapi)

A few folders and data files must be created on the device.  Supposing HAPI_HOME is /home/pi/hapi/, the following files are needed under /home/pi/hapi:
* capabilities.json  the capabilities response. 
* catalog.json   the catalog response.  
* info   a directory containing the info responses for each dataset id.
* info/10.CF3744000800.json  the info response
* data/10.CF3744000800/2018/10.CF3744000800.20180110.csv  a granule of the data set.
* data/10.CF3744000800/2018/10.CF3744000800.20180111.csv  another granule of the data set.

Note these are available in the zip file hapi_home.zip.  This can be unzipped and used to test the server.

# Requirements
* There was a module needed to run on my Pi, because it didn't have dateutil.parser.  This was fixed with:
    sudo apt-get install python-pip
    sudo pip install python-dateutil
