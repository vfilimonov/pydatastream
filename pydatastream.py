import pandas as pd
from suds.client import Client

WSDL_URL = 'http://dataworks.thomson.com/Dataworks/Enterprise/1.0/webserviceclient.asmx?WSDL'


class DatastreamException(Exception):
    pass


class Datastream:
    def __init__(self, username, password, url=WSDL_URL):
        """Creating connection to the Thomson Reuters Dataworks Enterprise (DWE) server
           (former Thomson Reuters Datastream).
        """
        self.client = Client(url, username=username, password=password)

        ### Trying to connect
        try:
            self.ver = self.version()
        except:
            raise DatastreamException('Can not retrieve the data')

        ### Creating UserData object
        self.userdata = self.client.factory.create('UserData')
        self.userdata.Username = username
        self.userdata.Password = password

    def version(self):
        """Return version of the TR DWE."""
        res = self.client.service.Version()
        return '.'.join([str(x) for x in res[0]])

    def system_info(self):
        """Return system information."""
        res = self.client.service.SystemInfo()
        res = {str(x[0]):x[1] for x in res[0]}

        to_str = lambda arr: '.'.join([str(x) for x in arr[0]])
        res['OSVersion'] = to_str(res['OSVersion'])
        res['RuntimeVersion'] = to_str(res['RuntimeVersion'])
        res['Version'] = to_str(res['Version'])

        res['Name'] = str(res['Name'])
        res['Server'] = str(res['Server'])
        res['LocalNameCheck'] = str(res['LocalNameCheck'])
        res['UserHostAddress'] = str(res['UserHostAddress'])

        return res

    def sources(self):
        """Return available sources of data."""
        res = self.client.service.Sources(self.userdata, 0)
        return [str(x[0]) for x in res[0]]

    def request(self, query, source='Datastream',
                fields=None, options=None, symbol_set=None, tag=None):
        """General function to retrieve one record in raw format.

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

        rd = self.client.factory.create('RequestData')
        rd.Source = source
        rd.Instrument = query
        if fields is not None:
            rd.Fields = self.client.factory.create('ArrayOfString')
            rd.Fields.string = fields
        rd.SymbolSet = symbol_set
        rd.Options = options
        rd.Tag = tag

        return self.client.service.RequestRecord(self.userdata, rd, 0)

    @staticmethod
    def parse_record(record, inline_metadata=False, raise_on_error=True):
        """Parse raw data (that is retrieved by "request") and return pandas.DataFrame.
           Returns tuple (data, metadata, status)

           inline_metadata - if True, then info about symbol, currency, frequency and
                             displayname will be included into dataframe with data.

           data - pandas.DataFrame with retrieved data.
           metadata - disctionary with info about symbol, currency, frequency and
                      displayname (if inline_metadata==True then this info is also
                      duplicated as fields in data)
           status - dictionary with data source, string with request and status type,
                    code and message.

           status['StatusType']: 'Connected' - the data is fine
                                 'Stale'     - the source is unavailable. It may be
                                               worthwhile to try again later
                                 'Failure'   - data could not be obtained (e.g. the
                                               instrument is incorrect)
                                 'Pending'   - for internal use only
           status['StatusCode']: 0 - 'No Error'
                                 1 - 'Disconnected'
                                 2 - 'Source Fault'
                                 3 - 'Network Fault'
                                 4 - 'Access Denied' (user does not have permissions)
                                 5 - 'No Such Item' (no instrument with given name)
                                 11 - 'Blocking Timeout'
                                 12 - 'Internal'
        """
        get_field = lambda name: [x[1] for x in record['Fields'][0] if x[0] == name][0]

        ### Parsing status
        status = {'Source': str(record['Source']),
                  'StatusType': str(record['StatusType']),
                  'StatusCode': record['StatusCode'],
                  'StatusMessage': str(record['StatusMessage']),
                  'Request': str(record['Instrument'])}

        ### Testing if no errors
        if status['StatusType'] != 'Connected':
            if raise_on_error:
                raise DatastreamException('%s (error %i): "%s"' %
                                          (status['StatusType'], status['StatusCode'],
                                           status['StatusMessage']))
            else:
                return pd.DataFrame(), {}, status

        ### Parsing metadata of the symbol
        metadata = {'Frequency': str(get_field('FREQUENCY')),
                    'Currency': str(get_field('CCY')),
                    'DisplayName': str(get_field('DISPNAME')),
                    'Symbol': str(get_field('SYMBOL'))}

        ### Parsing retrieved fields
        fields = [str(x[0]) for x in record['Fields'][0]
                  if x[0] not in ['CCY', 'DISPNAME', 'FREQUENCY', 'SYMBOL', 'DATE']]

        data = pd.DataFrame({x:get_field(x)[0] for x in fields},
                            index=get_field('DATE')[0])
        if inline_metadata:
            for x in metadata:
                data[x] = metadata[x]

        return data, metadata, status