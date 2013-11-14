import pandas as pd
import datetime as dt
import warnings
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

        ### If true then request string will be printed
        self.show_request = False

    @staticmethod
    def info():
        print 'Datastream Navigator:'
        print 'http://product.datastream.com/navigator/'
        print ''
        print 'Datastream documentation:'
        print 'http://dtg.tfn.com/data/DataStream.html'
        print ''
        print 'Dataworks Enterprise documentation:'
        print 'http://dataworks.thomson.com/Dataworks/Enterprise/1.0/'

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

    #====================================================================================
    @staticmethod
    def parse_record(record, inline_metadata=False, raise_on_error=True):
        """Parse raw data (that is retrieved by "request") and return pandas.DataFrame.
           Returns tuple (data, metadata, status)

           inline_metadata - if True, then info about symbol, currency, frequency and
                             displayname will be included into dataframe with data.
           raise_on_error - if True then error request will raise, otherwise either
                            empty dataframe or partially retrieved data will be returned

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
                raise DatastreamException('%s (error %i): %s --> "%s"' %
                                          (status['StatusType'], status['StatusCode'],
                                           status['StatusMessage'], status['Request']))
            else:
                return pd.DataFrame(), {}, status

        error = [str(x[1]) for x in record['Fields'][0] if 'INSTERROR' in x[0]]
        if len(error)>0:
            if raise_on_error:
                raise DatastreamException('Error: %s --> "%s"' %
                                          (error,
                                           status['Request']))
            else:
                status['StatusMessage'] = error
                status['StatusType'] = 'INSTERROR'
                metadata = {}
        else:
            ### Parsing metadata of the symbol
            ### NB! currency might be returned as symbol thus "unicode" shoud be used
            metadata = {'Frequency': str(get_field('FREQUENCY')),
                        'Currency': unicode(get_field('CCY')),
                        'DisplayName': unicode(get_field('DISPNAME')),
                        'Symbol': str(get_field('SYMBOL'))}

        ### Fields with data
        meta_fields = ['CCY', 'DISPNAME', 'FREQUENCY', 'SYMBOL', 'DATE']
        fields = [str(x[0]) for x in record['Fields'][0]
                  if (x[0] not in meta_fields and 'INSTERROR' not in x[0])]

        ### Check if we have a single value or a series
        if isinstance(get_field('DATE'), dt.datetime):
            data = pd.DataFrame({x:[get_field(x)] for x in fields},
                                index=[get_field('DATE')])
        else:
            data = pd.DataFrame({x:get_field(x)[0] for x in fields},
                                index=get_field('DATE')[0])

        ### Incorporate metadata to dataframe if required
        if inline_metadata:
            for x in metadata:
                data[x] = metadata[x]
        return data, metadata, status

    @staticmethod
    def construct_request(ticker, fields=None, date=None,
                          date_from=None, date_to=None, freq=None):
        """Construct a request string for querying TR DWE.

           tickers - ticker or symbol
           fields  - list of fields.
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)
           freq    - frequency of data: daily('D'), weekly('W') or monthly('M')

           Some of available fields:
           P  - adjusted closing price
           PO - opening price
           PH - high price
           PL - low price
           VO - volume, which is expressed in 1000's of shares.
           UP - unadjusted price
           OI - open interest

           MV - market value
           EPS - earnings per share
           DI - dividend index
           MTVB - market to book value
           PTVB - price to book value
           ...

           The full list of data fields is available at http://dtg.tfn.com/.
        """
        request = ticker
        if fields is not None:
            if isinstance(fields, str):
                request += '~='+fields
            elif isinstance(fields, list) and len(fields)>0:
                request += '~='+','.join(fields)
        if date is not None:
            request += '~@'+pd.to_datetime(date).strftime('%Y-%m-%d')
        else:
            if date_from is not None:
                request += '~'+pd.to_datetime(date_from).strftime('%Y-%m-%d')
            if date_to is not None:
                request += '~:'+pd.to_datetime(date_to).strftime('%Y-%m-%d')
        if freq is not None:
            request += '~'+freq
        return request

    #====================================================================================
    def fetch(self, tickers, fields=None, date=None,
              date_from=None, date_to=None, freq='D', raise_on_error=True, only_data=True):
        """Fetch data from TR DWE.

           tickers - ticker or symbol
           fields  - list of fields.
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)
           freq    - frequency of data: daily('D'), weekly('W') or monthly('M')
           raise_on_error - if True then error request will raise, otherwise either
                            empty dataframe or partially retrieved data will be returned
           only_data - if True then metadata and status data will not be returned

           Some of available fields:
           P  - adjusted closing price
           PO - opening price
           PH - high price
           PL - low price
           VO - volume, which is expressed in 1000's of shares.
           UP - unadjusted price
           OI - open interest

           MV - market value
           EPS - earnings per share
           DI - dividend index
           MTVB - market to book value
           PTVB - price to book value
           ...

           The full list of data fields is available at http://dtg.tfn.com/.
        """
        if isinstance(tickers, str):
            tickers = [tickers]

        ### TODO: request multiple tickers
        query = self.construct_request(tickers[0], fields, date, date_from, date_to, freq)
        if self.show_request:
            print 'Request:', query
        raw = self.request(query)
        (data, meta, status) = self.parse_record(raw, raise_on_error=raise_on_error)

        ### TODO: format metadata and return
        if only_data:
            return data
        else:
            return data, meta, status

    #====================================================================================
    def get_OHLCV(self, ticker, date=None, date_from=None, date_to=None):
        """Get Open, High, Low, Close prices and daily Volume for a given ticker.

           ticker  - ticker or symbol
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)

           Returns pandas.Dataframe with data. If error occurs, then it is printed as
           a warning.
        """
        (data, meta, status) = self.fetch(ticker+"~OHLCV", None,
                                          date, date_from, date_to, 'D',
                                          raise_on_error=False, only_data=False)
        if status['StatusType'] != 'Connected':
            if isinstance(status['StatusMessage'], str):
                warnings.warn('[DWE] ' + status['StatusMessage'])
            elif isinstance(status['StatusMessage'], list):
                warnings.warn('[DWE] ' + ';'.join(status['StatusMessage']))
        return data

    def get_OHLC(self, ticker, date=None, date_from=None, date_to=None):
        """Get Open, High, Low and Close prices for a given ticker.

           ticker  - ticker or symbol
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)

           Returns pandas.Dataframe with data. If error occurs, then it is printed as
           a warning.
        """
        (data, meta, status) = self.fetch(ticker+"~OHLC", None,
                                          date, date_from, date_to, 'D',
                                          raise_on_error=False, only_data=False)
        if status['StatusType'] != 'Connected':
            if isinstance(status['StatusMessage'], str):
                warnings.warn('[DWE] ' + status['StatusMessage'])
            elif isinstance(status['StatusMessage'], list):
                warnings.warn('[DWE] ' + ';'.join(status['StatusMessage']))
        return data

    def get_price(self, ticker, date=None, date_from=None, date_to=None):
        """Get Close price for a given ticker.

           ticker  - ticker or symbol
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)

           Returns pandas.Dataframe with data. If error occurs, then it is printed as
           a warning.
        """
        (data, meta, status) = self.fetch(ticker, None,
                                          date, date_from, date_to, 'D',
                                          raise_on_error=False, only_data=False)
        if status['StatusType'] != 'Connected':
            if isinstance(status['StatusMessage'], str):
                warnings.warn('[DWE] ' + status['StatusMessage'])
            elif isinstance(status['StatusMessage'], list):
                warnings.warn('[DWE] ' + ';'.join(status['StatusMessage']))
        return data