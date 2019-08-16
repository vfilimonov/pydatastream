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

_URL = 'https://product.datastream.com/dswsclient/V1/DSService.svc/rest/'

_FLDS_XREF = ('DSCD,EXMNEM,GEOG,GEOGC,IBTKR,INDC,INDG,INDM,INDX,INDXEG,'
              'INDXFS,INDXL,INDXS,ISIN,ISINID,LOC,MNEM,NAME,SECD,TYPE'.split(','))

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

Datastream Web Service Developer community
https://developers.refinitiv.com/eikon-apis/datastream-web-service
"""


###############################################################################
###############################################################################
# def _ustr(x):
#     """Unicode-safe version of str()"""
#     try:
#         return str(x)
#     except UnicodeEncodeError:
#         return unicode(x)

def _convert_date(date):
    """ Convert date to YYYY-MM-DD """
    if date is None:
        return ''
    else:
        return pd.Timestamp(date).strftime('%Y-%m-%d')


def _parse_dates(dates):
    """ Parse dates
        Example:
            1565817068486       -> 2019-08-14T21:11:08.486000000
            1565568000000+0000  -> 2019-08-12T00:00:00.000000000
    """
    if dates is None:
        return None
    res = pd.Series(dates).str[6:-2].str.replace('+0000', '', regex=False)
    res = pd.to_datetime(res.astype(float), unit='ms').values
    return pd.Timestamp(res[0]) if isinstance(dates, basestring) else res


class DatastreamException(Exception):
    pass


###############################################################################
# Main Datastream class
###############################################################################
class Datastream(object):
    def __init__(self, username, password, raise_on_error=True, proxy=None, **kwargs):
        """Establish a connection to the Python interface to the Refinitiv Datastream
           (former Thomson Reuters Datastream) API via Datastream Web Services (DSWS).

           username / password - credentials for the DSWS account.
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
    def _api_post(self, method, request):
        """ Call to the POST method of DSWS API """
        url = self._url + method
        self.last_request = {'url': url, 'request': request, 'error': None}
        try:
            res = requests.post(url, json=request, proxies=self._proxy)
            self.last_request['response'] = res.text
        except Exception as e:
            self.last_request['error'] = str(e)
            raise

        try:
            response = self.last_request['response'] = json.loads(self.last_request['response'])
        except json.JSONDecodeError:
            raise DatastreamException('Server response could not be parsed')

        if 'Code' in response:
            code = response['Code']
            if response['SubCode'] is not None:
                code += '/' + response['SubCode']
            errormsg = f'{code}: {response["Message"]}'
            self.last_request['error'] = errormsg
            raise DatastreamException(errormsg)
        return self.last_request['response']

    ###########################################################################
    def renew_token(self, username=None, password=None):
        """ Request new token from the server """
        if username is None or password is None:
            warnings.warn('Username or password is not provided - could not renew token')
            return
        data = {"UserName": username, "Password": password}
        self._token = dict(self._api_post('GetToken', data))
        self._token['TokenExpiry'] = _parse_dates(self._token['TokenExpiry'])

    @property
    def _token_is_expired(self):
        if self._token is None:
            return True
        # We invalidate token 15 minutes before expiration time
        if self._token['TokenExpiry'] < pd.Timestamp('now') - pd.Timedelta('15m'):
            return True
        return False

    @property
    def token(self):
        """ Return actual token and renew it if necessary. """
        if self._token_is_expired:
            self.renew_token(self._username, self._password)
        return self._token['TokenValue']

    ###########################################################################
    def request(self, request):
        """ Generic wrapper to request data in raw format. Request should be
            properly formatted dictionary (see construct_request() method).
        """
        data = {'DataRequest': request, 'TokenValue': self.token}
        return self._api_post('GetData', data)

    def request_many(self, list_of_requests):
        """ Generic wrapper to request multiple requests in raw format.
            list_of_requests should be a list of properly formatted dictionaries
            (see construct_request() method).
        """
        data = {'DataRequests': list_of_requests, 'TokenValue': self.token}
        return self._api_post('GetDataBundle', data)

    ###########################################################################
    @staticmethod
    def construct_request(ticker, fields=None, date_from=None, date_to=None,
                          freq=None, static=False, IsExpression=None,
                          return_names=True):
        """Construct a request string for querying TR DSWS.

           tickers - ticker or symbol, or list of symbols
           fields  - field or list of fields.
           date_from, date_to - date range (used only if "date" is not specified)
           freq    - frequency of data: daily('D'), weekly('W') or monthly('M')
           static  - True for static (snapshot) requests
           IsExpression - if True, it will explicitly assume that list of tickers
                          contain expressions. Otherwise it will try to infer it.

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
        req = {'Instrument': {}, 'Date': {}, 'DataTypes': []}

        # Instruments
        if isinstance(ticker, basestring):
            ticker = ticker
            IsList = None
        elif hasattr(ticker, '__len__'):
            ticker = ','.join(ticker)
            IsList = True
        else:
            raise ValueError('ticker should be either string or list/array of strings')
        # Properties of instruments
        props = []
        if IsList or (',' in ticker):
            props.append({'Key': 'IsList', 'Value': True})
        if IsExpression or ('#' in ticker or '(' in ticker or ')' in ticker):
            props.append({'Key': 'IsExpression', 'Value': True})
        if return_names:
            props.append({'Key': 'ReturnName', 'Value': True})
        req['Instrument'] = {'Value': ticker, 'Properties': props}

        # DataTypes
        props = [{'Key': 'ReturnName', 'Value': True}] if return_names else []
        if fields is not None:
            if isinstance(fields, basestring):
                req['DataTypes'].append({'Value': fields, 'Properties': props})
            elif isinstance(fields, list) and len(fields) > 0:
                for f in fields:
                    req['DataTypes'].append({'Value': f, 'Properties': props})
            else:
                raise ValueError('fields should be either string or list/array of strings')

        # Dates
        req['Date'] = {'Start': _convert_date(date_from),
                       'End': _convert_date(date_to),
                       'Frequency': freq if freq is not None else '',
                       'Kind': 0 if static else 1}
        return req

    ###########################################################################
    def _parse_one(self, res):
        data = res['DataTypeValues']
        dates = _parse_dates(res['Dates'])
        res_meta = {_: res[_] for _ in res if _ not in ['DataTypeValues', 'Dates']}

        # Parse values
        meta = {}
        res = {}
        for d in data:
            data_type = d['DataType']
            res[data_type] = {}
            meta[data_type] = {}

            for v in d['SymbolValues']:
                value = v['Value']
                if v['Type'] == 0:  # Error
                    if self.raise_on_error:
                        raise DatastreamException(value)
                    else:
                        res[data_type][v['Symbol']] = pd.np.NaN
                elif v['Type'] == 4:  # Date
                    res[data_type][v['Symbol']] = _parse_dates(value)
                else:
                    res[data_type][v['Symbol']] = value
                meta[data_type][v['Symbol']] = {_: v[_] for _ in v if _ != 'Value'}

            res[data_type] = pd.DataFrame(res[data_type], index=dates)

        res = pd.concat(res).unstack(level=1).T.sort_index()
        res_meta['Currencies'] = meta
        return res, res_meta

    def parse_response(self, response, return_metadata=False):
        """ Parse raw JSON response

            If return_metadata is True, then result is tuple (dataframe, metadata),
            where metadata is a dictionary. Otherwise only dataframe is returned.

            In case of response being constructed from several requests (method
            request_many()), then the result is a list of parsed responses. Here
            again, if return_metadata is True then each element is a tuple
            (dataframe, metadata), otherwise each element is a dataframe.
        """
        if 'DataResponse' in response:  # Single request
            res, meta = self._parse_one(response['DataResponse'])
            self._last_response_meta = meta
            return (res, meta) if return_metadata else res

        elif 'DataResponses' in response:  # Multiple requests
            results = [self._parse_one(r) for r in response['DataResponses']]
            self._last_response_meta = [_[1] for _ in results]
            return results if return_metadata else [_[0] for _ in results]

    ###########################################################################
    def usage_statistics(self, date=None):
        """ Request usage statistics """
        return self.fetch('STATS', 'DS.USERSTATS', date, static=True).T

    #################################################################################
    def fetch(self, tickers, fields=None, date_from=None, date_to=None,
              freq=None, static=False, IsExpression=None, return_metadata=False):
        """Fetch the data from Datastream for a set of tickers and parse results.

           tickers - ticker or symbol, or list of symbols
           fields  - field or list of fields
           date_from, date_to - date range (used only if "date" is not specified)
           freq    - frequency of data: daily('D'), weekly('W') or monthly('M')
           static  - True for static (snapshot) requests
           IsExpression - if True, it will explicitly assume that list of tickers
                          contain expressions. Otherwise it will try to infer it.

           Notes: - several fields should be passed as a list, and not as a
                    comma-separated string!
                  - if no fields are provided, then the default field will be
                    fetched. In this case the column might not have any name
                    in the resulting dataframe.

           Result format depends on the number of requested tickers and fields:
             - 1 ticker         - DataFrame with fields in column names
             - many tickers     - DataFrame with fields in column names and
                                  MultiIndex (ticker, date)
             - static request   - DataFrame indexed by tickers and with fields
                                  in column names

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
        """
        req = self.construct_request(tickers, fields, date_from, date_to,
                                     freq=freq, static=static,
                                     IsExpression=IsExpression,
                                     return_names=return_metadata)
        raw = self.request(req)
        self._last_response_raw = raw
        data, meta = self.parse_response(raw, return_metadata=True)

        if static:
            # Static request - drop date from MultiIndex
            data = data.reset_index(level=1, drop=True)
        elif len(data.index.levels[0]) == 1:
            # Only one ticker - drop tickers from MultiIndex
            data = data.reset_index(level=0, drop=True)

        return (data, meta) if return_metadata else data

    #################################################################################
    def get_OHLCV(self, ticker, date_from=None, date_to=None):
        """Get Open, High, Low, Close prices and daily Volume for a given ticker.

           ticker  - ticker or symbol
           date_from, date_to - date range (used only if "date" is not specified)
        """
        return self.fetch(ticker, ['PO', 'PH', 'PL', 'P', 'VO'], date_from, date_to,
                          freq='D', return_metadata=False)

    def get_OHLC(self, ticker, date=None, date_from=None, date_to=None):
        """Get Open, High, Low and Close prices for a given ticker.

           ticker  - ticker or symbol
           date_from, date_to - date range (used only if "date" is not specified)
        """
        return self.fetch(ticker, ['PO', 'PH', 'PL', 'P'], date_from, date_to,
                          freq='D', return_metadata=False)

    def get_price(self, ticker, date_from=None, date_to=None):
        """Get Close price for a given ticker.

           ticker  - ticker or symbol
           date_from, date_to - date range (used only if "date" is not specified)
        """
        return self.fetch(ticker, 'P', date_from, date_to,
                          freq='D', return_metadata=False)

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
        if only_list:
            fields = ['MNEM', 'NAME']
        else:
            fields = _FLDS_XREF
        return self.fetch('L' + index_ticker, fields, date_from=date, static=True)

    def get_all_listings(self, ticker):
        """ Get all listings and their symbols for the given security
        """
        res = self.fetch(ticker, 'QTEALL', static=True)
        columns = list(set([_[:2] for _ in res.columns]))

        # Reformat the output
        df = {}
        for ind in range(1, 21):
            cols = {f'{c}{ind:02d}': c for c in columns}
            df[ind] = res[cols.keys()].rename(columns=cols)
        df = pd.concat(df).swaplevel(0).sort_index()
        df = df[~(df == '').all(axis=1)]
        return df

    def get_codes(self, ticker):
        """ Get codes and symbols for the given securities
        """
        return self.fetch(ticker, _FLDS_XREF, static=True)

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

            >> DS.get_epit_vintage_matrix('USGDP...D', date_from='2015-01-01')

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
            data = self.fetch(mnemonic, 'RELH50', date_from=period, static=True)
        else:
            data = self.fetch(mnemonic, 'RELH', date_from=period, static=True)
        data = data.iloc[0]

        # Parse the response
        res = {data.loc['RELHD%02d' % i]: data.loc['RELHV%02d' % i]
               for i in range(1, 51 if relh50 else 21)
               if data.loc['RELHD%02d' % i] != ''}
        res = pd.Series(res, name=data.loc['RELHP  ']).sort_index()
        return res
