# PyDatastream

PyDatastream is a Python interface to the Refinitiv Datastream (former Thomson Reuters Datastream) API via Datastream Web Services (DSWS) (non free), with some extra convenience functions. This package requires valid credentials for this API.

**Note**: Up until version 0.5.x the library has been using SOAP API of DataWorksEnterprise (DWE). As of July 1, 2019 this interface was discontinued by Thompson Reuters, and at the moment Datastream content is delivered through Datastream Web Services (DSWS). Starting version 0.6 pydatastream library is using REST API of DSWS.

## Installation

The latest version of PyDatastream is always available at [GitHub](https://github.com/vfilimonov/pydatastream) at the `master` branch. Last release could be also installed from [PyPI](https://pypi.python.org/pypi/PyDatastream) using `pip`:

	pip install pydatastream

Two external dependencies are [pandas](http://pandas.pydata.org/pandas-docs/stable/install.html) and [requests](https://2.python-requests.org/en/master/).

## Basic use

All methods to work with the DWE is organized as a class, so first you need to create an object with your valid credentials:

    from pydatastream import Datastream
    DWE = Datastream(username="XXXX000", password="XXX000")

If necessary, the proxy server could be specified here via extra `proxy` parameter (e.g. `proxy='proxyLocation:portNumber'`). If authentication was successful, then you can check out system information (including the version of the DWE):

    DWE.system_info()

and list of data sources available for your subscription:

	DWE.sources()

Basic functionality of the module allows to fetch Open-High-Low-Close (OHLC) prices and Volumes for given ticker.

### Daily data

The following command requests daily closing price data for the Apple asset (DWE mnemonic `"@AAPL"`) on May 3, 2000:

	data = DWE.get_price('@AAPL', date='2000-05-03')
	print data

or request daily closing price data for Apple in 2008:

	data = DWE.get_price('@AAPL', date_from='2008', date_to='2009')


The data is retrieved as a `pandas.DataFrame` object, which can be easilly plotted:

	data.plot()

aggregated into monthly data:

	print data.resample('M', how='last')

or [manipulated in a variety of ways](http://nbviewer.ipython.org/urls/raw.github.com/changhiskhan/talks/master/pydata2012/pandas_timeseries.ipynb). Due to extreme simplicity of resampling  the data in Pandas library (for example, taking into account business calendar), I would recommend to request daily data (unless the requests are huge or daily scale is not applicable) and perform all transformations locally. Also note that thanks to Pandas library format of the date string is extremely flexible.


For fetching Open-High-Low-Close (OHLC) data there exist two methods: `get_OHLC` to fetch only price data and `get_OHLCV` to fetch both price and volume data. This separation is required as volume data is not available for financial indices.

Request daily OHLC and Volume data for Apple in 2008:

	data = DWE.get_OHLCV('@AAPL', date_from='2008', date_to='2009')

Request daily OHLC data for S&P 500 Index from May 6, 2010 until present date:

	data = DWE.get_OHLC('S&PCOMP', date_from='May 6, 2010')

### Requesting specific fields for data

If the Thomson Reuters mnemonic for specific fields are known, then more general function can be used. The following request

	data = DWE.fetch('@AAPL', ['P','MV','VO',], date_from='2000-01-01')

fetches the closing price, daily volume and market valuation for Apple Inc.

### Requesting several tickers at once

`fetch` can be used for requesting data for several tickers at once. In this case a MultiIndex Dataframe will be returned (see [Pandas: MultiIndex / Advanced Indexing](https://pandas.pydata.org/pandas-docs/stable/advanced.html)).

	res = DWE.fetch(['@AAPL','U:MMM'], fields=['P','MV','VO','PH'], date_from='2000-05-03')
	print res['MV'].unstack(level=0)

The resulting data frame could be sliced, in order to select all fields for a given ticker:

	print res.loc['U:MMM'].head()

or data for the specific field for all tickers:

	print res['MV'].unstack(level=0).head()

#### Note 1: Default field

**Important**: This is the most likely case of "E100, INVALID CODE OR EXPRESSION ENTERED" error message when fetching multiple symbols at once, even if they could be fetched one-by-one. This was observed so far in [exchange rates](https://github.com/vfilimonov/pydatastream/issues/14) and [economic](https://github.com/vfilimonov/pydatastream/issues/16) [series](https://github.com/vfilimonov/pydatastream/issues/11).

There's a slight ambiguity of what "P" stands for.
For cash equities there's a datatype "P" which correspond to adjusted price.
However when one does the request to an API, "P" also stands for the default field. And if no fields (datatypes) are supplied, API will assume that the default field "P" is requested.

It looks like, that when one requests several symbols, API will treat the `"P"` (even if it is implied, i.e. no datatypes are specified) strictly as a datatype. So if the datatype "P" does not exist (e.g. for exchange rates: "EUDOLLR" or equity indices: "S&PCOMP") the request will be resulting in an error: e.g. here `$$"ER", E100, INVALID CODE OR EXPRESSION ENTERED, USDOLLR(P)`.

So in order retrieve several symbols at once, proper datatypes should be specified. For example:

	res = DWE.fetch(['EUDOLLR','USDOLLR'], fields=['EB','EO'])


#### Note 2: error catching

As it will be discussed below, it may be convenient to set up property `raise_on_error` to `False` when fetching data of several tickers. In this case if one or several tickers are misspecified, error will no be raised, but they will not be present in the output data frame. Further, if some field does not exist for some of tickers, but exists for others, the missing data will be replaced with NaNs:

	DWE.raise_on_error = False
	res = DWE.fetch(['@AAPL','U:MMM','xxxxxx','S&PCOMP'], fields=['P','MV','VO','PH'], date_from='2000-05-03')

	print res['MV'].unstack(level=0).head()
	print res['P'].unstack(level=0).head()

Please note, that in the last example the closing price (`P`) for `S&PCOMP` ticker was not retrieved. Due to Thomson Reuters Datastream mnemonics, field `P` is not available for indexes and field `PI` should be used instead.

### Constituents list for indices

PyDatastream also has an interface for retrieving list of constituents of indices:

	res = DWE.get_constituents('S&PCOMP')
	print res.ix[0]

As an option, the list for a specific date can be requested as well:

	res = DWE.get_constituents('S&PCOMP', '1-sept-2013')

By default the method retrieves many various mnemonics and codes for the constituents,
which might pose a problem for large indices like Russel-3000 (the request might be killed
on timeout). In this case one can request only symbols and company names using:

	res = DWE.get_constituents('FRUSS3L', only_list=True)

### Static requests

List of constituents of indices, that were considered above, is an example of static request, i.e. a request that does not retrieve a time-series, but a single snapshot with the data.

On the API level static requests are marked with "~REP" suffix. Within the PyDatastream library static requests could be called using the same `fetch()` function as time series by providing an argument `static=True`.

For example, this request retrieves names, ISINs and identifiers for primary exchange (ISINID) for various tickers of BASF corporation:

	res = DWE.fetch(['D:BAS','D:BASX','HN:BAS','I:BAF','BFA','@BFFAF','S:BAS'],
                    ['ISIN', 'ISINID', 'NAME'], static=True)

Another example of use of static requests is a cross-section of time-series. The following example retrieves an actual price, market capitalization and daily volume for same companies:

	res = DWE.fetch(['D:BAS','D:BASX','HN:BAS','I:BAF','BFA','@BFFAF','S:BAS'],
                    ['P', 'MV', 'VO'], static=True)

## Advanced use

### Some useful functionality of Datastream

Some of examples are taken from [Thomson Financial Network](http://dtg.tfn.com/data/DataStream.html) and [description of rDatastream package](https://github.com/fcocquemas/rdatastream).

Get some reference information on a security with `"~XREF"`, including ISIN, industry, etc.

	res = DWE.request('U:IBM~XREF')
	print DWE.extract_data(res)

Convert the currency e.g. to Euro with `"~~EUR"`

    res = DWE.fetch('U:IBM(P)~~EUR', date_from='2013-09-01')
    print res.head()

### Datastream Functions

Datastream also allows to apply a number of functions to the series, which are caculated on the server side. Given the flexibility of pandas library in all types of data transformation, this functionality is not really needed for python users. However I will briefly describe it for the sake of completeness.

Functions have a format `FUNC#(mnemonic,parameter)`. For example, calculating moving average on 20 days on the prices of IBM:

	res = DWE.fetch('MAV#(U:IBM,20D)', date_from='2013-09-01')

Functions could be combined, e.g. calculating moving 3 day percentage change on 20 days moving average:

	res = DWE.fetch('PCH#(MAV#(U:IBM,20D),3D)', date_from='2013-09-01')

Calculate percentage quarter-on-quarter change for the US real GDP (constant prices, seasonally adjusted):

	res = DWE.fetch('PCH#(USGDP...D,1Q)', date_from='1990-01-01')

Calculate year-on-year difference (actual change) for the UK real GDP (constant prices, seasonally adjusted):

	res = DWE.fetch('ACH#(UKGDP...D,1Y)', date_from='1990-01-01')

Documentation on the available functions are available on the [Thompson Reuters webhelp](http://product.datastream.com/navigator/advancehelpfiles/functions/webhelp/hfunc.htm).


### Using custom requests

The module has a general-purpose function `request` that can be used for fetching data with custom requests. This function returns raw data in format of `suds` package. Data can be used directly or parsed later with the `parse_record` method:

	raw = DWE.request('@AAPL~=P,MV,VO,PH~2013-01-01~D')
	print raw['StatusType']
	print raw['StatusCode']

	data = DWE.extract_data(raw)
	print data['CCY']
	print data['MV']

Information about mnemonics and syntax of the request string can be found in [Thomson Financial Network](http://dtg.tfn.com/data/DataStream.html).

### Performing several requests at once

[Thomson Dataworks Enterprise User Guide](http://dataworks.thomson.com/Dataworks/Enterprise/1.0/documentation/user%20guide.pdf) suggests to optimize requests: very often it is quicker to make one bigger request than several smaller requests because of the relatively high transport overhead with web services.

PyDatastream allows to fetch several requests in a single API call:

	r1 = '@AAPL~OHLCV~2013-11-26~D'
	r2 = 'U:MMM~=P,MV,PO~2013-11-26~D'
	res = DWE.request_many([r1,r2])

	print DWE.parse_record(res[0])
	print DWE.parse_record(res[1])

Please note, that due to possible specifics of requests, results of them should be parsed separately, similar to the example above.

### Debugging and error handling

If request contains errors then normally `DatastreamException` will be raised and the error message from the DWE will be printed. To alter this behavior, one can use `raise_on_error` property of Datastream class. Being set to `False` it will force parser to ignore error messages and return empty pandas.Dataframe. For instance:

	r1 = '@AAPL~OHLCV~2013-11-26~D'
	r2 = '902172~OHLCV~wrong_request'
	res = DWE.request_many([r1,r2])

	DWE.raise_on_error = False
	print DWE.parse_record(res[0])
	print DWE.parse_record(res[1])

`raise_on_error` can be useful for requests that contain several tickers. In this case data fields and/or tickers that can not be fetched will not be present in the output. However please be aware, that this is not a silver bullet against all types of errors. In all complicated cases it is suggested to go with one symbol at a time and check the raw response from the server to spot invalid combinations of ticker-field.

For the debugging purposes, Datastream class has `show_request` property, which, if set to `True`, makes standard methods to output the text string with request:

	DWE.show_request = True
	data = DWE.fetch('@AAPL', ['P','MV','VO',], date_from='2000-01-01')

Method `status` could extract status info from the record with raw response:

	print DWE.status(res[1])

and `last_status` property always contains status of the last parsed record:

	print DWE.last_status

In many cases the errors occur at the stage of parsing the raw response from the server.
For example, this could happen if some fields requested are not supported for all symbols
in the request. Because the data returned from the server in the unstructured form and the
parser tries to map this to the structured pandas.DataFrame, the exceptions could be
relatively uninformative. In this case it is possible to check the raw response from the
server to spot the problematic field. The last raw response is stored in the `last_response`
field:

	print DWE.last_response

## Thomson Reuters Economic Point-in-Time (EPiT) functionality

PyDatastream has two useful methods to work with the Thomson Reuters Economic Point-in-Time (EPiT) concept (usually a separate subscription is required for this content). Most of the economic time series, such as GDP, employment or inflation figures are undergoing many revisions on their way. So that US GDP value undertakes two consecutive revisions after the initial release, after which the number is revised periodically when e.g. either the base year of calculation or seasonal adjustment are changed. For predictive exercies it is important to obtain the actual values as they were known at the time of releases (otherwise the data will be contaminated by look-ahead bias).

EPiT allows user to request such historical data. For example, the following method retrieves the initial estimate and first revisions of the 2010-Q1 US GDP, as well as the dates when the data was published:

	res = DWE.get_epit_revisions('USGDP...D', period='2010-02-15')

The period here should contain a date which falls within a time period of interest (so any date from '2010-01-01' to '2010-03-31' will result in the same output).

Another useful concept is a concept of "vintage" of a data, which defines when the particular series was released. This concept is widely used in economics, see for example [ALFRED database](https://alfred.stlouisfed.org/) of St. Louis Fed and their discussions about [capturing data as it happens](https://alfred.stlouisfed.org/docs/alfred_capturing_data.pdf).

All vintages of the economic indicator could be summarized in a vintage matrix, that represents a DataFrame where columns correspond to a particular period (quarter or month) for the reported statistic and index represents timestamps at which these values were released by the respective official agency. I.e. every line corresponds to all available reported values by the given date.

For example for the US GDP:

	res = DWE.get_epit_vintage_matrix('USGDP...D', date_from='2015-01-01')

The response is:

	            2015-02-15  2015-05-15  2015-08-15  2015-11-15  \
	2015-04-29    16304.80         NaN         NaN         NaN
	2015-05-29    16264.10         NaN         NaN         NaN
	2015-06-24    16287.70         NaN         NaN         NaN
	2015-07-30    16177.30   16270.400         NaN         NaN
	2015-08-27    16177.30   16324.300         NaN         NaN
	2015-09-25    16177.30   16333.600         NaN         NaN
	2015-10-29    16177.30   16333.600   16394.200         NaN
	2015-11-24    16177.30   16333.600   16417.800         NaN
	...

From the matrix it is seen for example, that the advance GDP estimate
for 2015-Q1 (corresponding to 2015-02-15) was released on 2015-04-29
and was equal to 16304.80 (B USD). The first revision (16264.10) has happened
on 2015-05-29 and the second (16287.70) - on 2015-06-24. On 2015-07-30
the advance GDP figure for 2015-Q2 was released (16270.400) together
with update on the 2015-Q1 value (16177.30) and so on.


## Resources

- [Datastream Navigator](http://product.datastream.com/navigator/) could be used to look up codes and data types.

- [Official support webpage](https://customers.reuters.com/sc/Contactus/simple?product=Datastream&env=PU&TP=Y).

- [Official webpage for testing REST API requests](http://product.datastream.com/dswsclient/Docs/TestRestV1.aspx)

- [Documentation for DSWS API calls](http://product.datastream.com/dswsclient/Docs/Default.aspx)

Finally, all these links could be printed in your terminal or iPython notebook by calling

	DWE.info()


### Datastream Navigator

[Datastream Navigator](http://product.datastream.com/navigator/) is the best place to search for mnemonics for a particular security or a list of available datatypes/fields.

Once a necessary security is located (either by search or via "Explore" menu item), a short list of most frequently used mnemonics is located right under the chart with the series. Further the small button ">>" next to them open a longer list.

The complete list of dataypes (includig static) for a particular asset class is located in the "Datatype search" menu item. Here the proper asset class should be selected.

Help for Datastream Navigator is available [here](http://product.datastream.com/WebHelp/Navigator/4.5/HelpFiles/Nav_help.htm).


## Notes

* The package is using [Pandas](http://pandas.pydata.org/) library ([GitHub repo](https://github.com/pydata/pandas)), which I found to be the best Python library for time series manipulations. Together with [IPython notebook](http://ipython.org/notebook.html) it is the best open source tool for the data analysis. For quick start with pandas have a look on [tutorial notebook](http://nbviewer.ipython.org/urls/gist.github.com/fonnesbeck/5850375/raw/c18cfcd9580d382cb6d14e4708aab33a0916ff3e/1.+Introduction+to+Pandas.ipynb) and [10-minutes introduction](http://pandas.pydata.org/pandas-docs/stable/10min.html).
* Alternatives for other scientific computing languages:
  - MATLAB: [MATLAB datafeed toolbox](http://www.mathworks.fr/help/toolbox/datafeed/datastream.html)
  - R: [RDatastream](https://github.com/fcocquemas/rdatastream) (in fact PyDatastream was inspired by RDatastream).
* I am always open for suggestions, critique and bug reports.

## Important (backward incompatible) changes

* Starting version 0.5.0 the methods `fetch()` for many tickers returns MultiIndex Dataframe instead of former Panel. This follows the development of the Pandas library where the Panels are deprecated starting version 0.20.0 (see [here](http://pandas.pydata.org/pandas-docs/version/0.20/whatsnew.html#whatsnew-0200-api-breaking-deprecate-panel)).
* As of July 1, 2019 old DataWorksEnterprise (DWE) interfaces (that were used by pydatastream of versions up to 0.5.1) were discontinued by Thompson Reuters. Starting version 0.6 pydatastream uses Datastream Web Services (DSWS) interfaces.
* Starting version 0.6 the library is no longer guaranteed to support Python 2.
* Starting version 0.6 the following methods do not exist any more: `version`, `system_info`, `sources` (belong to a discontinued SOAP API); `request` and `request_many` (given that there no query strings anymore, all functionality could be accessed via `fetch` method)

## Acknowledgements

A special thanks to:

* François Cocquemas ([@fcocquemas](https://github.com/fcocquemas)) for  [RDatastream](https://github.com/fcocquemas/rdatastream) that has inspired this library
* Charles Allderman ([@ceaza](https://github.com/ceaza)) for added support of Python 3


## License

PyDatastream library is released under the MIT license.

The license for the library is not extended in any sense to any of the content of the Thomson Reuters Dataworks Enterprise, Datastream, Datastream Navigator or related services. Appropriate contract with Thomson Reuters and valid credentials are required in order to use the API.

Author of the library ([@vfilimonov](https://github.com/vfilimonov)) is not affiliated, associated, authorized, sponsored, endorsed by, or in any way officially connected with Thomson Reuters, or any of its subsidiaries or its affiliates. The name “Thomson Reuters” as well as related names are registered trademarks of Thomson Reuters.
