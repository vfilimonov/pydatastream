import pandas as pd
import datetime as dt
import warnings
import json
import requests

# Python3-safe basesctring Method
# http://www.rfk.id.au/blog/entry/preparing-pyenchant-for-python-3/
try:
    unicode = unicode
except NameError:
    # 'unicode' is undefined, must be Python 3
    str = str
    unicode = str
    bytes = bytes
    basestring = (str, bytes)
else:
    # 'unicode' exists, must be Python 2
    str = str
    unicode = unicode
    bytes = str
    basestring = basestring

# TODO: QTEALL: all available active tickers for the company (e.g. "U:IBM~=QTEALL~REP")

_URL = 'http://product.datastream.com/dswsclient/V1/DSService.svc/rest/'

_INFO = """PyDatastream documentation (GitHub):
https://github.com/vfilimonov/pydatastream

Datastream Navigator:
http://product.datastream.com/navigator/

Official support
https://customers.reuters.com/sc/Contactus/simple?product=Datastream&env=PU&TP=Y

Webpage for testing REST API requests
http://product.datastream.com/dswsclient/Docs/TestRestV1.aspx

Documentation for DSWS API
http://product.datastream.com/dswsclient/Docs/Default.aspx
"""


###############################################################################
###############################################################################
# def _ustr(x):
#     """Unicode-safe version of str()"""
#     try:
#         return str(x)
#     except UnicodeEncodeError:
#         return unicode(x)


class DatastreamException(Exception):
    pass


###############################################################################
# Main Datastream class
###############################################################################
class Datastream(object):
    def __init__(self, username, password, raise_on_error=True, proxy=None, **kwargs):
        """Establish a connection to the Python interface to the Refinitiv Datastream
           (former Thomson Reuters Datastream) API via Datastream Web Services (DSWS).

           username / password - credentials for the DWE account.
           raise_on_error - If True then error request will raise a "DatastreamException",
                            otherwise either empty dataframe or partially
                            retrieved data will be returned
           proxy - URL for the proxy server. Valid values:
                   (a) None: no proxy is used
                   (b) string of format "host:port" or "username:password@host:port"

           Note: credentials will be saved in memory. In case if this is not
                 desirable for security reasons, call the constructor having None
                 instead of values and manually call renew_token(username, password)
                 when needed.

           A custom REST API url (if necessary for some reasons) could be provided
           via "url" parameter.
        """
        self.raise_on_error = raise_on_error
        self.last_request = None  # Data from the last request

        # Setting up proxy parameters if necessary
        if isinstance(proxy, basestring):
            self._proxy = {'http': proxy, 'https': proxy}
        elif proxy is None:
            self._proxy = None
        else:
            raise ValueError('Proxy parameter should be either None or string')

        self._url = kwargs.pop('url', _URL)
        self._username = username
        self._password = password
        # request new token
        self.renew_token(username, password)

    ###########################################################################
    @staticmethod
    def info():
        """ Some useful links """
        print(_INFO)

    ###########################################################################
    def _api_post(self, url, request):
        """ Call to the POST method of DSWS API """
        self._last_request = {'url': url, 'request': request, 'error': None}
        try:
            res = requests.post(url, json=request, proxies=self._proxy)
            self._last_request['response'] = res.text
        except Exception as e:
            self._last_request['error'] = str(e)
            raise
        try:
            self._last_request['response'] = json.loads(self._last_request['response'])
            return self._last_request['response']
        except json.JSONDecodeError:
            raise DatastreamException('Server response could not be parsed')

    ###########################################################################
    def renew_token(self, username=None, password=None):
        """ Request new token from the server """
        if username is None or password is None:
            warngins.warn('Username or password is not provided - could not renew token')
            return
        url = self._url + 'GetToken'
        data = {"UserName": username, "Password": password}
        res = self._api_post(url, data)
        if 'Code' in res:
            code = res['Code']
            if res['SubCode'] is not None:
                code += '/' + res['SubCode']
            raise DatastreamException(f'{code}: {res["Message"]}')
        self._token = res

    @property
    def _token_is_expired(self):
        if self._token is None:
            return True
        # TODO: Check if token is expired according to the TokenExpiry field
        return True

    @property
    def token(self):
        """ Return actual token and renew it if necessary. """
        if self._token_is_expired:
            self.renew_token(self._username, self._password)
        return self._token

    ###########################################################################
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
        if self.show_request:
            try:
                print('Request:' + query)
            except UnicodeEncodeError:
                print('Request:' + query.encode('utf-8'))

        rd = self.client.factory.create('RequestData')
        rd.Source = source
        rd.Instrument = query
        if fields is not None:
            rd.Fields = self.client.factory.create('ArrayOfString')
            rd.Fields.string = fields
        rd.SymbolSet = symbol_set
        rd.Options = options
        rd.Tag = tag

        self.last_response = self.client.service.RequestRecord(self.userdata, rd, 0)

        return self.last_response

    def request_many(self, queries, source='Datastream',
                     fields=None, options=None, symbol_set=None, tag=None):
        """General function to retrieve one record in raw format.

           query - list of query strings for DWE system.
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
           NB! source, options, symbol_set and tag are assumed to be identical for all
               requests in the list
        """
        if self.show_request:
            print(('Requests:', queries))

        if not isinstance(queries, list):
            queries = [queries]

        req = self.client.factory.create('ArrayOfRequestData')
        req.RequestData = []
        for q in queries:
            rd = self.client.factory.create('RequestData')
            rd.Source = source
            rd.Instrument = q
            if fields is not None:
                rd.Fields = self.client.factory.create('ArrayOfString')
                rd.Fields.string = fields
            rd.SymbolSet = symbol_set
            rd.Options = options
            rd.Tag = tag

            req.RequestData.append(rd)

        return self.client.service.RequestRecords(self.userdata, req, 0)[0]

    #################################################################################
    def status(self, record=None):
        """Extract status from the retrieved data and save it as a property of an object.
           If record with data is not specified then the status of previous operation is
           returned.

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
        if record is not None:
            self.last_status = {'Source': ustr(record['Source']),
                                'StatusType': ustr(record['StatusType']),
                                'StatusCode': record['StatusCode'],
                                'StatusMessage': ustr(record['StatusMessage']),
                                'Request': ustr(record['Instrument'])}
        return self.last_status

    def _test_status_and_warn(self):
        """Test status of last request and post warning if necessary.
        """
        status = self.last_status
        if status['StatusType'] != 'Connected':
            if isinstance(status['StatusMessage'], basestring):
                warnings.warn('[DWE] ' + status['StatusMessage'])
            elif isinstance(status['StatusMessage'], list):
                warnings.warn('[DWE] ' + ';'.join(status['StatusMessage']))

    #################################################################################
    @staticmethod
    def extract_data(raw):
        """Extracts data from the raw response and returns it as a dictionary."""
        return {x[0]: x[1] for x in raw['Fields'][0]}

    def parse_record(self, raw, indx=0):
        """Parse raw data (that is retrieved by "request") and return pandas.DataFrame.
           Returns tuple (data, metadata)

           data - pandas.DataFrame with retrieved data.
           metadata - pandas.DataFrame with info about symbol, currency, frequency,
                      displayname and status of given request
        """
        suffix = '' if indx == 0 else '_%i' % (indx + 1)

        # Parsing status
        status = self.status(raw)

        # Testing if no errors
        if status['StatusType'] != 'Connected':
            if self.raise_on_error:
                raise DatastreamException('%s (error %i): %s --> "%s"' %
                                          (status['StatusType'], status['StatusCode'],
                                           status['StatusMessage'], status['Request']))
            else:
                self._test_status_and_warn()
                return pd.DataFrame(), {}

        record = self.extract_data(raw)
        get_field = lambda fldname: record[fldname + suffix]

        try:
            error = get_field('INSTERROR')
            if self.raise_on_error:
                raise DatastreamException('Error: %s --> "%s"' %
                                          (error, status['Request']))
            else:
                self.last_status['StatusMessage'] = error
                self.last_status['StatusType'] = 'INSTERROR'
                self._test_status_and_warn()
                metadata = {'Frequency': '', 'Currency': '', 'DisplayName': '',
                            'Symbol': '', 'Status': error}
        except KeyError:
            # Parsing metadata of the symbol
            # NB! currency might be returned as symbol thus "unicode" should be used
            metadata = {'Frequency': ustr(get_field('FREQUENCY')),
                        'Currency': ustr(get_field('CCY')),
                        'DisplayName': ustr(get_field('DISPNAME')),
                        'Symbol': ustr(get_field('SYMBOL')),
                        'Status': 'OK'}

        # Fields with data
        if suffix == '':
            fields = [ustr(x) for x in record if '_' not in x]
        else:
            fields = [ustr(x) for x in record if suffix in x]

        # Filter metadata
        meta_fields = ['CCY', 'DISPNAME', 'FREQUENCY', 'SYMBOL', 'DATE', 'INSTERROR']
        fields = [x.replace(suffix, '') for x in fields
                  if not any([y in x for y in meta_fields])]

        if 'DATE' + suffix in record:
            date = record['DATE' + suffix]
        elif 'DATE' in record:
            date = record['DATE']
        else:
            date = None

        if len(fields) > 0 and date is not None:
            # Check if we have a single value or a series
            if isinstance(date, dt.datetime):
                data = pd.DataFrame({x: [get_field(x)] for x in fields},
                                    index=[date])
            else:
                data = pd.DataFrame({x: get_field(x)[0] for x in fields},
                                    index=date[0])
        else:
            data = pd.DataFrame()

        metadata = pd.DataFrame(metadata, index=[indx])
        metadata = metadata[['Symbol', 'DisplayName', 'Currency', 'Frequency', 'Status']]
        return data, metadata

    def parse_record_static(self, raw):
        """Parse raw data (that is retrieved by static request) and return pandas.DataFrame.
           Returns tuple (data, metadata)

           data - pandas.DataFrame with retrieved data.
           metadata - pandas.DataFrame with info about symbol, currency, frequency,
                      displayname and status of given request
        """
        # Parsing status
        status = self.status(raw)

        # Testing if no errors
        if status['StatusType'] != 'Connected':
            if self.raise_on_error:
                raise DatastreamException('%s (error %i): %s --> "%s"' %
                                          (status['StatusType'], status['StatusCode'],
                                           status['StatusMessage'], status['Request']))
            else:
                self._test_status_and_warn()
                return pd.DataFrame(), {}

        # Convert record to dict
        record = self.extract_data(raw)

        try:
            error = record['INSTERROR']
            if self.raise_on_error:
                raise DatastreamException('Error: %s --> "%s"' %
                                          (error, status['Request']))
            else:
                self.last_status['StatusMessage'] = error
                self.last_status['StatusType'] = 'INSTERROR'
                self._test_status_and_warn()
                return pd.DataFrame(), {'Status': error, 'Date': None}
        except KeyError:
            metadata = {'Status': 'OK', 'Date': ''}

        # All fields that are available
        fields = [x for x in record if '_' not in x]
        metadata['Date'] = record['DATE']
        fields.remove('DATE')

        # Number of elements
        num = len([x[0] for x in record if 'SYMBOL' in x])

        # field naming 'CCY', 'CCY_2', 'CCY_3', ...
        fld_name = lambda field, indx: field if indx == 0 else field + '_%i' % (indx + 1)

        # Construct pd.DataFrame
        res = pd.DataFrame({fld: [record[fld_name(fld, ind)]
                                  if fld_name(fld, ind) in record else ''
                                  for ind in range(num)]
                            for fld in fields})
        return res, metadata

    #################################################################################
    @staticmethod
    def construct_request(ticker, fields=None, date=None,
                          date_from=None, date_to=None, freq=None):
        """Construct a request string for querying TR DWE.

           tickers - ticker or symbol
           fields  - list of fields.
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)
           freq    - frequency of data: daily('D'), weekly('W') or monthly('M')
                     Use here 'REP' for static requests

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
        if isinstance(ticker, basestring):
            request = ticker
        elif hasattr(ticker, '__len__'):
            request = ','.join(ticker)
        else:
            raise ValueError('ticker should be either string or list/array of strings')
        if fields is not None:
            if isinstance(fields, basestring):
                request += '~=' + fields
            elif isinstance(fields, list) and len(fields) > 0:
                request += '~=' + ','.join(fields)
        if date is not None:
            request += '~@' + pd.to_datetime(date).strftime('%Y-%m-%d')
        else:
            if date_from is not None:
                request += '~' + pd.to_datetime(date_from).strftime('%Y-%m-%d')
            if date_to is not None:
                request += '~:' + pd.to_datetime(date_to).strftime('%Y-%m-%d')
        if freq is not None:
            request += '~' + freq
        return request

    #################################################################################
    def fetch(self, tickers, fields=None, date=None, date_from=None, date_to=None,
              freq='D', only_data=True, static=False):
        """Fetch data from TR DWE.

           tickers - ticker or list of tickers
           fields  - list of fields.
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)
           freq    - frequency of data: daily('D'), weekly('W') or monthly('M')
           only_data - if True then metadata will not be returned
           static  - if True "static" request is created (i.e. not a series).
                     In this case 'date_from', 'date_to' and 'freq' are ignored

           In case list of tickers is requested, a MultiIndex-dataframe is returned.

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
        if static:
            query = self.construct_request(tickers, fields, date, freq='REP')
        else:
            query = self.construct_request(tickers, fields, date, date_from, date_to, freq)

        raw = self.request(query)

        if static:
            data, metadata = self.parse_record_static(raw)
        elif isinstance(tickers, basestring) or len(tickers) == 1:
            data, metadata = self.parse_record(raw)
        elif hasattr(tickers, '__len__'):
            metadata = pd.DataFrame()
            data = {}
            for indx in range(len(tickers)):
                dat, meta = self.parse_record(raw, indx)
                data[tickers[indx]] = dat
                metadata = metadata.append(meta, ignore_index=False)

            data = pd.concat(data)
        else:
            raise DatastreamException(('First argument should be either ticker or '
                                       'list of tickers'))

        if only_data:
            return data
        else:
            return data, metadata

    #################################################################################
    def get_OHLCV(self, ticker, date=None, date_from=None, date_to=None):
        """Get Open, High, Low, Close prices and daily Volume for a given ticker.

           ticker  - ticker or symbol
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)

           Returns pandas.Dataframe with data. If error occurs, then it is printed as
           a warning.
        """
        data, meta = self.fetch(ticker + "~OHLCV", None, date,
                                date_from, date_to, 'D', only_data=False)
        return data

    def get_OHLC(self, ticker, date=None, date_from=None, date_to=None):
        """Get Open, High, Low and Close prices for a given ticker.

           ticker  - ticker or symbol
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)

           Returns pandas.Dataframe with data. If error occurs, then it is printed as
           a warning.
        """
        data, meta = self.fetch(ticker + "~OHLC", None, date,
                                date_from, date_to, 'D', only_data=False)
        return data

    def get_price(self, ticker, date=None, date_from=None, date_to=None):
        """Get Close price for a given ticker.

           ticker  - ticker or symbol
           date    - date for a single-date query
           date_from, date_to - date range (used only if "date" is not specified)

           Returns pandas.Dataframe with data. If error occurs, then it is printed as
           a warning.
        """
        data, meta = self.fetch(ticker, None, date,
                                date_from, date_to, 'D', only_data=False)
        return data

    #################################################################################
    def get_constituents(self, index_ticker, date=None, only_list=False):
        """ Get a list of all constituents of a given index.

            index_ticker - Datastream ticker for index
            date         - date for which list should be retrieved (if None then
                           list of present constituents is retrieved)
            only_list    - request only list of symbols. By default the method
                           retrieves many extra fields with information (various
                           mnemonics and codes). This might pose some problems
                           for large indices like Russel-3000. If only_list=True,
                           then only the list of symbols and names are retrieved.
        """
        if date is not None:
            str_date = pd.to_datetime(date).strftime('%m%y')
        else:
            str_date = ''
        # Note: ~XREF is equal to the following large request
        # ~REP~=DSCD,EXMNEM,GEOG,GEOGC,IBTKR,INDC,INDG,INDM,INDX,INDXEG,INDXFS,INDXL,
        #       INDXS,ISIN,ISINID,LOC,MNEM,NAME,SECD,TYPE
        fields = '~REP~=NAME' if only_list else '~XREF'
        query = 'L' + index_ticker + str_date + fields
        raw = self.request(query)

        res, metadata = self.parse_record_static(raw)
        return res

    #################################################################################
    def get_epit_vintage_matrix(self, mnemonic, date_from='1951-01-01', date_to=None):
        """ Construct the vintage matrix for a given economic series.
            Requires subscription to Thomson Reuters Economic Point-in-Time (EPiT).

            Vintage matrix represents a DataFrame where columns correspond to a
            particular period (quarter or month) for the reported statistic and
            index represents timestamps at which these values were released by
            the respective official agency. I.e. every line corresponds to all
            available reported values by the given date.

            For example:

            >> DWE.get_epit_vintage_matrix('USGDP...D', date_from='2015-01-01')

                        2015-02-15  2015-05-15  2015-08-15  2015-11-15  \
            2015-04-29    16304.80         NaN         NaN         NaN
            2015-05-29    16264.10         NaN         NaN         NaN
            2015-06-24    16287.70         NaN         NaN         NaN
            2015-07-30    16177.30   16270.400         NaN         NaN
            2015-08-27    16177.30   16324.300         NaN         NaN
            2015-09-25    16177.30   16333.600         NaN         NaN
            2015-10-29    16177.30   16333.600   16394.200         NaN
            2015-11-24    16177.30   16333.600   16417.800         NaN

            From the matrix it is seen for example, that the advance GDP estimate
            for 2015-Q1 (corresponding to 2015-02-15) was released on 2015-04-29
            and was equal to 16304.80 (B USD). The first revision (16264.10) has
            happened on 2015-05-29 and the second (16287.70) - on 2015-06-24.
            On 2015-07-30 the advance GDP figure for 2015-Q2 was released
            (16270.400) together with update on the 2015-Q1 value (16177.30)
            and so on.
        """
        # Get first available date from the REL1 series
        rel1 = self.fetch(mnemonic, 'REL1', date_from=date_from, date_to=date_to)
        date_0 = rel1.dropna().index[0]

        # All release dates
        reld123 = self.fetch(mnemonic, ['RELD1', 'RELD2', 'RELD3'],
                             date_from=date_0, date_to=date_to).dropna(how='all')

        # Fetch all vintages
        res = {}
        for date in reld123.index:
            try:
                _tmp = self.fetch(mnemonic, 'RELV', date_from=date_0, date_to=date).dropna()
            except DatastreamException:
                continue
            res[date] = _tmp
        return pd.concat(res).RELV.unstack()

    #################################################################################
    def get_epit_revisions(self, mnemonic, period, relh50=False):
        """ Return initial estimate and first revisions of a given economic time
            series and a given period.
            Requires subscription to Thomson Reuters Economic Point-in-Time (EPiT).

            "Period" parameter should represent a date which falls within a time
            period of interest, e.g. 2016 Q4 could be requested with the
            period='2016-11-15' for example.

            By default up to 20 values is returned unless argument "relh50" is
            set to True (in which case up to 50 values is returned).
        """
        if relh50:
            data = self.fetch(mnemonic, 'RELH50', date=period, static=True)
        else:
            data = self.fetch(mnemonic, 'RELH', date=period, static=True)
        data = data.iloc[0]

        # Parse the response
        res = {data.loc['RELHD%02d' % i]: data.loc['RELHV%02d' % i]
               for i in range(1, 51 if relh50 else 21)
               if data.loc['RELHD%02d' % i] != ''}
        res = pd.Series(res, name=data.loc['RELHP  ']).sort_index()
        return res
