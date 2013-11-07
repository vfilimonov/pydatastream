
import pandas
from suds.client import Client

WSDL_URL = 'http://dataworks.thomson.com/Dataworks/Enterprise/1.0/webserviceclient.asmx?WSDL'

class datastream:
    def __init__(self, username, password, url=WSDL_URL):
        """Creating connection to the Thomson Reuters Dataworks Enterprise (DWE) server
           (former Thomson Reuters Datastream).
        """
        self.client = Client(url, username=username, password=password)

        ### Trying to connect
        try:
            self.ver = self.version()
        except:
            raise Exception('datastream: can not retrieve the data')

        ### Creating UserData object
        self.userdata = self.client.factory.create('UserData')
        self.userdata.Username = username
        self.userdata.Password = password

    def version(self):
        """Return version of the TR DWE."""
        res = self.client.service.Version()
        return res[0]

    def system_info(self):
        """Return system information."""
        res = self.client.service.SystemInfo()
        res = {x[0]:x[1] for x in res[0]}
        res['OSVersion'] = res['OSVersion'][0]
        res['RuntimeVersion'] = res['RuntimeVersion'][0]
        res['Version'] = res['Version'][0]
        return res

    def sources(self):
        """Return available sources of data."""
        res = self.client.service.Sources(self.userdata, 0)
        return [x[0] for x in res[0]]

    def request_record(self, instrument, fields, source='Datastream',
                       options=None, symbol_set=None, tag=None):
        """General function to retrieve one record."""

        RD = self.client.factory.create('RequestData')
        RD.Source = source
        RD.Instrument = instrument
        RD.Fields = fields
        RD.SymbolSet = symbol_set
        RD.Options = options
        RD.Tag = tag

        return self.client.service.RequestRecord(self.userdata, RD, 0)

