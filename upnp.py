import urllib2
import socket
import logging
from xml.etree import ElementTree as ET

DESCRIPTION_NS = 'urn:schemas-upnp-org:device-1-0'
ENVELOPE_NS = 'http://schemas.xmlsoap.org/soap/envelope/'

#class Response:
#    def __init__(self,msg):
#        self.msg = msg
#        self.headers = {}
#        self.body = {}
#        self.parse()
#
#    def parse(self):
#        m = self.msg.splitlines()
#        body = []

class UPNP:
    def __init__(self):
        self.logger = logging.getLogger('tamchy.UPNP')
        self.controlUrls = {}
        self.description_xml = ''
        self.gateway_addr = ''
        self.service_available = ''
        self.connection_established = False
        self.init()

    def init(self):
        s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        # header for searching InternetGatewayDevice -> routers,modems
        R = '''M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: ssdp:discover\r\nMX: 10\r\nST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\r\n'''
        s.sendto(R,('239.255.255.250',1900))
        s.settimeout(5)
        try:
            data,addr = s.recvfrom(4096)
        except socket.timeout:
            return 
        response = {}
        for line in data.splitlines():
            # extracting http-headers
            if not line:
                break
            header, value = line.split(':',1) if ':' in line else (line,'')
            response[header.lower()] = value
        # split('//') == splitting http:// -> split('/') == splitting everything after ip:port -> split(':') == splitting ip and port value
        gateway = response['location'].split('//')[1].split('/')[0]
        self.gateway_ip, self.gateway_port = gateway.split(':') if ':' in gateway else (gateway,'80')
        self.gateway_addr = 'http://%s:%s' % (self.gateway_ip,self.gateway_port)
        try:
            self.description_xml = urllib2.urlopen(response['location']).read()
        except:
            self.logger.debug('Cannot get Device Description XML from IGD. Response form IGD --> ' + data)
            return
        xml = ET.fromstring(self.description_xml)
        for s in xml.iter('{%s}service' % (DESCRIPTION_NS)):
            service = s.find('{%s}serviceType' % (DESCRIPTION_NS)).text.split(':',3)[-1]
            controlUrl = s.find('{%s}controlURL' % (DESCRIPTION_NS)).text
            self.controlUrls[service] = controlUrl
        self.service_available = 'WANPPPConnection:1' if ('WANPPPConnection:1' in self.controlUrls) else 'WANIPConnection:1'
        self.connection_established = True
    
    def get_envelope(self,service,action,**kw):
        return """<?xml version="1.0"?>
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
                <s:Body>
                    <u:%s xmlns:u="urn:schemas-upnp-org:service:%s">""" % (action, service) +\
                     "\n".join(["<%s>%s</%s>"%(k,v,k) for k,v in kw.items()]) +\
                """ </u:%s>
                </s:Body>
            </s:Envelope>""" % action

    def request(self,service,action,**kw):
        if not self.connection_established:
            return 
        req = urllib2.Request(self.gateway_addr + self.controlUrls[service])
        envelope = self.get_envelope(service,action,**kw)
        req.add_header("content-type",'text/xml; charset="utf-8"')
        req.add_header("SOAPACTION", '"urn:schemas-upnp-org:service:%s#%s"' % (service, action))
        print(envelope)
        req.add_data(envelope)
        try: 
            response = urllib2.build_opener().open(req)
        except: 
            return 
        return response

    def get_external_ip(self):
        response = self.request('WANIPConnection:1','GetExternalIPAddress')
        if response and response.getcode() == 200:
            xml = response.read()
            if xml:
                root = ET.fromstring(xml)
                ext_ip = root.find('.//NewExternalIPAddress')
                # if tag is founded
                if ext_ip is not None:
                    return ext_ip.text
        return ''

    def get_generic_port_mapping_entry(self,index):
        service_available = self.service_available
        response = self.request(service_available,'GetGenericPortMappingEntry',NewPortMappingIndex=index)
        d = {}
        if response and response.getcode() == 200:
            xml = response.read()
            print('xml --> ' + xml)
            if xml:
                root = ET.fromstring(xml)
                body = root.find('{%s}BODY' % (ENVELOPE_NS))
                if body is not None:
                    resp = body.find('{%s}GetGenericPortMappingEntryResponse' % ('urn:schemas-upnp-org:service:' + service_available))
                    if resp is not None:
                        for el in list(resp):
                            d[el.tag] = el.text
        return d

    def get_specific_port_mapping_entry(self,external_port,remote_host='',protocol='TCP'):
        service_available = self.service_available
        response = self.request(service_available,'GetSpecificPortMappingEntry',NewRemoteHost=remote_host,
                                                                                NewExternalPort=external_port,
                                                                                NewProtocol=protocol)
        d = {}
        print(response.getcode())
        print(response)
        if response and response.getcode() == 200:
            xml = response.read()
            print('xml --> ' + xml)
            if xml:
                root = ET.fromstring(xml)
                body = root.find('{%s}BODY' % (ENVELOPE_NS))
                if body is not None:
                    resp = body.find('{%s}GetSpecificPortMappingEntryResponse' % ('urn:schemas-upnp-org:service:' + service_available))
                    if resp is not None:
                        for el in list(resp):
                            d[el.tag] = el.text
        return d



    def add_port_mapping(self,external_port,internal_port,protocol='TCP',duration=0,remote_host='',description='tamchy_port_mapping'):
        # we will try to use WANPPP first because in ADSL modems this service must be used -> if it's a router we'll use WANIP
        service_available = self.service_available
        # getting our internal ip from IGD
        s = socket.socket()
        try:s.connect((self.gateway_ip,80))
        except: return False
        this_host = s.getsockname()[0]
        s.close()
        response = self.request(service_available,'AddPortMapping',NewRemoteHost=remote_host, NewExternalPort=external_port,
                                                                   NewInternalPort=internal_port, 
                                                                   NewInternalClient=this_host, NewProtocol=protocol, 
                                                                   NewPortMappingDescription=description, NewEnabled='1',
                                                                   NewLeaseDuration=duration)
        if response and response.getcode() == 200:
            print(response.read())
            return True
        # otherwise
        return False

    def delete_port_mapping(self,external_port,protocol='TCP',remote_host=''):
        service_available = self.service_available
        response = self.request(service_available,'DeletePortMapping',NewRemoteHost=remote_host, NewExternalPort=external_port, 
                                                                                        NewProtocol=protocol)
        if response and response.getcode() == 200:
            return True
        return False

if __name__  == '__main__':
    u = UPNP()
    print(u.get_external_ip())
