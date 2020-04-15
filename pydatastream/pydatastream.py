""" pydatastream main module

    (c) Vladimir Filimonov, 2013 - 2020
"""
# pylint: disable=C0103,R0902,R0904,R0913,C0330
import warnings
import json
import math
from functools import wraps
import requests
import pandas as pd

###############################################################################
_URL = 'https://product.datastream.com/dswsclient/V1/DSService.svc/rest/'

_FLDS_XREF = ('DSCD,EXMNEM,GEOGC,GEOGN,IBTKR,INDC,INDG,INDM,INDX,INDXEG,'
              'INDXFS,INDXL,INDXS,ISIN,ISINID,LOC,MNEM,NAME,SECD,TYPE'.split(','))

_FLDS_XREF_FUT = ('MNEM,NAME,FLOT,FEX,GEOGC,GEOGN,EXCODE,LTDT,FUTBDATE,PCUR,ISOCUR,'
                  'TICKS,TICKV,TCYCLE,TPLAT'.split(','))

_ASSET_TYPE_CODES = {'BD': 'Bonds & Convertibles',
                     'BDIND': 'Bond Indices & Credit Default Swaps',
                     'CMD': 'Commodities',
                     'EC': 'Economics',
                     'EQ': 'Equities',
                     'EQIND': 'Equity Indices',
                     'EX': 'Exchange Rates',
                     'FT': 'Futures',
                     'INT': 'Interest Rates',
                     'INVT': 'Investment Trusts',
                     'OP': 'Options',
                     'UT': 'Unit Trusts',
                     'EWT': 'Warrants',
                     'NA': 'Not available'}

###############################################################################
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
def _convert_date(date):
    """ Convert date to YYYY-MM-DD """
    if date is None:
        return ''
    if isinstance(date, str) and (date.upper() == 'BDATE'):
        return 'BDATE'
    return pd.Timestamp(date).strftime('%Y-%m-%d')


def _parse_dates(dates):
    """ Parse dates
        Example:
            /Date(1565817068486)       -> 2019-08-14T21:11:08.486000000
            /Date(1565568000000+0000)  -> 2019-08-12T00:00:00.000000000
    """
    if dates is None:
        return None
    if isinstance(dates, str):
        return pd.Timestamp(_parse_dates([dates])[0])
    res = [int(_[6:(-7 if '+' in _ else -2)]) for _ in dates]
    return pd.to_datetime(res, unit='ms').values


class DatastreamException(Exception):
    """ Exception class for Datastream """


###############################################################################
def lazy_property(fn):
    """ Lazy-evaluated property of an object """
    attr_name = '__lazy__' + fn.__name__

    @property
    @wraps(fn)
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property


###############################################################################
# Main Datastream class
###############################################################################
class Datastream():
    """ Python interface to the Refinitiv Datastream API via Datastream Web
        Services (DSWS).
    """
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
        self.last_request = None
        self.last_metadata = None
        self._last_response_raw = None

        # Setting up proxy parameters if necessary
        if isinstance(proxy, str):
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
        self.last_metadata = None
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
        if isinstance(ticker, str):
            ticker = ticker
            is_list = None
        elif hasattr(ticker, '__len__'):
            ticker = ','.join(ticker)
            is_list = True
        else:
            raise ValueError('ticker should be either string or list/array of strings')
        # Properties of instruments
        props = []
        if is_list or (is_list is None and ',' in ticker):
            props.append({'Key': 'IsList', 'Value': True})
        if IsExpression or (IsExpression is None and
                            ('#' in ticker or '(' in ticker or ')' in ticker)):
            props.append({'Key': 'IsExpression', 'Value': True})
        if return_names:
            props.append({'Key': 'ReturnName', 'Value': True})
        req['Instrument'] = {'Value': ticker, 'Properties': props}

        # DataTypes
        props = [{'Key': 'ReturnName', 'Value': True}] if return_names else []
        if fields is not None:
            if isinstance(fields, str):
                req['DataTypes'].append({'Value': fields, 'Properties': props})
            elif isinstance(fields, list) and fields:
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
    @staticmethod
    def _parse_meta(meta):
        """ Parse SymbolNames, DataTypeNames and Currencies """
        res = {}
        for key in meta:
            if key in ('DataTypeNames', 'SymbolNames'):
                if not meta[key]:   # None or empty list
                    res[key] = None
                else:
                    names = pd.DataFrame(meta[key]).set_index('Key')['Value']
                    names.index.name = key.replace('Names', '')
                    names.name = 'Name'
                    res[key] = names
            elif key == 'Currencies':
                cur = pd.concat({key: pd.DataFrame(meta['Currencies'][key])
                                 for key in meta['Currencies']})
                res[key] = cur.xs('Currency', level=1).T
            else:
                res[key] = meta[key]
        return res

    def _parse_one(self, res):
        """ Parse one response (either 'DataResponse' or one of 'DataResponses')"""
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
                        raise DatastreamException(f'"{v["Symbol"]}"("{data_type}"): {value}')
                    res[data_type][v['Symbol']] = math.nan
                elif v['Type'] == 4:  # Date
                    res[data_type][v['Symbol']] = _parse_dates(value)
                else:
                    res[data_type][v['Symbol']] = value
                meta[data_type][v['Symbol']] = {_: v[_] for _ in v if _ != 'Value'}

            if dates is None:
                # Fix - if dates are not returned, then simply use integer index
                dates = [0]
            res[data_type] = pd.DataFrame(res[data_type], index=dates)

        res = pd.concat(res).unstack(level=1).T.sort_index()
        res_meta['Currencies'] = meta
        return res, self._parse_meta(res_meta)

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
            self.last_metadata = meta
            return (res, meta) if return_metadata else res
        if 'DataResponses' in response:  # Multiple requests
            results = [self._parse_one(r) for r in response['DataResponses']]
            self.last_metadata = [_[1] for _ in results]
            return results if return_metadata else [_[0] for _ in results]
        raise DatastreamException('Neither DataResponse nor DataResponses are found')

    ###########################################################################
    def usage_statistics(self, date=None, months=1):
        """ Request usage statistics

            date - if None, stats for the current month will be fetched,
                   otherwise for the month which contains the specified date.
            months - number of consecutive months prior to "date" for which
                     stats should be retrieved.
        """
        if date is None:
            date = pd.Timestamp('now').normalize() - pd.offsets.MonthBegin()
        req = [self.construct_request('STATS', 'DS.USERSTATS',
                                      date-pd.offsets.MonthBegin(n), static=True)
               for n in range(months)][::-1]
        res = self.parse_response(self.request_many(req))
        res = pd.concat(res)
        res.index = res['Start Date'].dt.strftime('%B %Y')
        res.index.name = None
        return res

    ###########################################################################
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

    ###########################################################################
    # Specific fetching methods
    ###########################################################################
    def get_OHLCV(self, ticker, date_from=None, date_to=None):
        """Get Open, High, Low, Close prices and daily Volume for a given ticker.

           ticker  - ticker or symbol
           date_from, date_to - date range (used only if "date" is not specified)
        """
        return self.fetch(ticker, ['PO', 'PH', 'PL', 'P', 'VO'], date_from, date_to,
                          freq='D', return_metadata=False)

    def get_OHLC(self, ticker, date_from=None, date_to=None):
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

    ###########################################################################
    def get_constituents(self, index_ticker, only_list=False):
        """ Get a list of all constituents of a given index.

            index_ticker - Datastream ticker for index
            only_list    - request only list of symbols. By default the method
                           retrieves many extra fields with information (various
                           mnemonics and codes). This might pose some problems
                           for large indices like Russel-3000. If only_list=True,
                           then only the list of symbols and names are retrieved.

            NOTE: In contrast to retired DWE interface, DSWS does not support
                  fetching historical lists of constituents.
        """
        if only_list:
            fields = ['MNEM', 'NAME']
        else:
            fields = _FLDS_XREF
        return self.fetch('L' + index_ticker, fields, static=True)

    def get_all_listings(self, ticker):
        """ Get all listings and their symbols for the given security
        """
        res = self.fetch(ticker, 'QTEALL', static=True)
        columns = list({_[:2] for _ in res.columns})

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

    ###########################################################################
    def get_asset_types(self, symbols):
        """ Get asset types for a given list of symbols
            Note: the method does not
        """
        res = self.fetch(symbols, 'TYPE', static=True, IsExpression=False)
        names = pd.Series(_ASSET_TYPE_CODES).to_frame(name='AssetTypeName')
        res = res.join(names, on='TYPE')
        # try to preserve the order
        if isinstance(symbols, (list, pd.Series)):
            try:
                res = res.loc[symbols]
            except:
                pass  # OK, we don't keep the order if not possible
        return res

    ###########################################################################
    def get_futures_contracts(self, market_code, only_list=False, include_dead=False):
        """ Get list of all contracts for a given futures market

            market_code  - Datastream mnemonic for a market (e.g. LLC for the
                           Brent Crude Oil, whose contracts have mnemonics like
                           LLC0118 for January 2018)
            only_list    - request only list of symbols. By default the method
                           retrieves many extra fields with information (currency,
                           lot size, last trading date, etc). If only_list=True,
                           then only the list of symbols and names are retrieved.
            include_dead - if True, all delisted/expired contracts will be fetched
                           as well. Otherwise only active contracts will be returned.
        """
        if only_list:
            fields = ['MNEM', 'NAME']
        else:
            fields = _FLDS_XREF_FUT
        res = self.fetch(f'LFUT{market_code}L', fields, static=True)
        res['active'] = True
        if include_dead:
            res2 = self.fetch(f'LFUT{market_code}D', fields, static=True)
            res2['active'] = False
            res = pd.concat([res, res2])
        return res[res.MNEM != 'NA']  # Drop lines with empty mnemonics

    ###########################################################################
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

    ###########################################################################
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

    ###########################################################################
    def get_next_release_dates(self, mnemonics, n_releases=1):
        """ Return the next date of release (NDoR) for a given economic series.
            Could return results for up to 12 releases in advance.

            Returned fields:
              DATE        - Date of release (or start of range where the exact
                            date is not available)
              DATE_LATEST - End of range when a range is given
              TIME_GMT    - Expected time of release (for "official" dates only)
              DATE_FLAG   - Indicates whether the dates are "official" ones from
                            the source, or estimated by Thomson Reuters where
                            "officials" not available
              REF_PERIOD  - Corresponding reference period for the release
              TYPE        - Indicates whether the release is for a new reference
                            period ("NewValue") or an update to a period for which
                            there has already been a release ("ValueUpdate")
        """
        if n_releases > 12:
            raise Exception('Only up to 12 months in advance could be requested')
        if n_releases < 1:
            raise Exception('n_releases smaller than 1 does not make sense')

        # Fetch and parse
        reqs = [self.construct_request(mnemonics, f'DS.NDOR{i+1}', static=True)
                for i in range(n_releases)]
        res_parsed = self.parse_response(self.request_many(reqs))

        # Rearrange the output
        res = []
        for r in res_parsed:
            x = r.reset_index(level=1, drop=True)
            x.index.name = 'Mnemonic'
            # Index of the release counting from now
            ndor_idx = [_.split('_')[0] for _ in x.columns][0]
            x['ReleaseNo'] = int(ndor_idx.replace('DS.NDOR', ''))
            x = x.set_index('ReleaseNo', append=True)
            x.columns = [_.replace(ndor_idx+'_', '') for _ in x.columns]
            res.append(x)

        res = pd.concat(res).sort_index()
        for col in ['DATE', 'DATE_LATEST', 'REF_PERIOD']:
            res[col] = pd.to_datetime(res[col], errors='coerce')
        return res

    ###########################################################################
    @lazy_property
    def vacations_list(self):
        """ List of mnemonics for holidays in different countries """
        res = self.fetch('HOLIDAYS', ['MNEM', 'ENAME', 'GEOGN', 'GEOGC'], static=True)
        return res[res['MNEM'] != 'NA'].sort_values('GEOGC')

    def get_trading_days(self, countries, date_from=None, date_to=None):
        """ Get list of trading dates for a given countries (speficied by ISO-2c)
            Returning dataframe will contain values 1 and NaN, where 1 identifies
            the business day.

            So by multiplying this list with the price time series it will remove
            padded values on non-trading days.

            Example:
                DS.get_trading_days(['US', 'UK', 'RS'], date_from='2010-01-01')
        """
        if isinstance(countries, str):
            countries = [countries]

        vacs = self.vacations_list
        mnems = vacs[vacs.GEOGC.isin(countries)]
        missing_isos = set(countries).difference(mnems.GEOGC)

        if missing_isos:
            raise DatastreamException(f'Unknowns ISO codes: {", ".join(missing_isos)}')
        # By default 0 and NaN are returned, so we add 1
        res = self.fetch(mnems.MNEM, date_from=date_from, date_to=date_to) + 1

        if len(countries) == 1:
            return res.iloc[:, 0].to_frame(name=countries[0])
        return (res.iloc[:, 0].unstack(level=0)
                   .rename(columns=mnems.set_index('MNEM')['GEOGC'])[countries])
