
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

    def request(self, query, source='Datastream',
                fields=None, options=None, symbol_set=None, tag=None):
        """General function to retrieve one record.

           query - query string for DWE system. This may be a simple instrument name
                   or more complicated request. Refer to the documentation for the
                   format.
           source - The name of datasource (default: "Datastream")
           fields - Fields to be retrieved (used when the requester does not want all
                    fields to be delivered).
           options - Options for specific data source. Many of datasources do not require
                     opptions string. Refer to the documentation of the specific
                     datasource for allowed syntax.
           symbol_set - The symbol set used inside the instrument (used for mapping
                        identifiers within the request. Refer to the documentation for
                        the details.
           tag - User-defined cookie that can be used to match up requests and response.
                 It will be returned back in the response. The string should not be
                 longer than 256 characters.
        """

        RD = self.client.factory.create('RequestData')
        RD.Source = source
        RD.Instrument = query
        if fields is not None:
            RD.Fields = self.client.factory.create('ArrayOfString')
            RD.Fields.string = fields
        RD.SymbolSet = symbol_set
        RD.Options = options
        RD.Tag = tag

        return self.client.service.RequestRecord(self.userdata, RD, 0)

