# PyDatastream

PyDatastream is a Python interface to the [Thomson Dataworks Enterprise](http://dataworks.thomson.com/Dataworks/Enterprise/1.0/) (DWE) SOAP API (non free), with some convenience functions for retrieving Datastream data specifically. This package requires valid credentials for this API.

## Notes

* This package is mainly meant to access Datastream. However basic functionality (```request``` method) should work for [other Dataworks Enterprise sources](http://dtg.tfn.com/data/).
* The package is using [Pandas](http://pandas.pydata.org/) library ([GitHub repo](https://github.com/pydata/pandas)), which I found to be the best Python library for time series manipulations. Together with [IPython notebook](http://ipython.org/notebook.html) it is the best open source tool for the data analysis. For quick start with pandas have a look on [tutorial notebook](http://nbviewer.ipython.org/urls/gist.github.com/fonnesbeck/5850375/raw/c18cfcd9580d382cb6d14e4708aab33a0916ff3e/1.+Introduction+to+Pandas.ipynb) and [10-minutes introduction](http://pandas.pydata.org/pandas-docs/stable/10min.html).
* Alternatives for other scientific computing languages:
  - MATLAB: [MATLAB datafeed toolbox](http://www.mathworks.fr/help/toolbox/datafeed/datastream.html)
  - R: [RDatastream](https://github.com/fcocquemas/rdatastream) (in fact PyDatastream was inspired by RDatastream).
* I am always open for suggestions, critique and bug reports.

## Installation

First, install prerequisites: `pandas` and `suds`. Both of packages can be installed with the [pip installer](http://www.pip-installer.org/en/latest/):

    pip install pandas
    pip install suds

However, please refer to the [pandas documentation](http://pandas.pydata.org/pandas-docs/stable/install.html) for the dependencies. 

PyDatastream could be also installed from [PyPI](https://pypi.python.org/pypi/PyDatastream/0.2.0) using `pip`:

	pip install pydatastream

## Basic use

All methods to work with the DWE is organized as a class, so first you need to create an object with your valid credentials:

    from pydatastream import Datastream
    DWE = Datastream(username="DS:XXXX000", password="XXX000")

If authentication was successfull, then you can check system information (including version of DWE):

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
	print data.head()

The data is retrieved as pandas.DataFrame object, which can be plotted:

	data.plot()
	
aggregated into monthly data:

	print data.resample('M', how='last')
	
or [manipulated in a variety of ways](http://nbviewer.ipython.org/urls/raw.github.com/changhiskhan/talks/master/pydata2012/pandas_timeseries.ipynb). Due to extreme simplicity of resampling  the data in Pandas library (for example, tainkg into account business calendar), I would recommend to request daily data (unless the requests are huge or daily scale is not applicable) and perform all transformations locally. Also note that thanks to Pandas library format of the date string is extremely flexible.


For fetching Open-High-Low-Close (OHLC) data there exist two methods: `get_OHLC` to fetch only price data and `get_OHLCV` to fetch boyth price and volume data. This separation is required as volume data is not available for financial indices.

Request daily OHLC and Volume data for Apple in 2008:

	data = DWE.get_OHLCV('@AAPL', date_from='2008', date_to='2009')
	
Request daily OHLC data for S&P 500 Index from May 6, 2010 until present date:

	data = DWE.get_OHLC('S&PCOMP', date_from='May 6, 2010')

### Requesting specific fields for data

If the Thomson Reuters mnemonic for specific fields are known, then more general function can be used. The following request

	data = DWE.fetch('@AAPL', ['P','MV','VO',], date_from='2000-01-01')

fetchs the closing price, daily volume and market valuation for Apple Inc.

### Requesting several tickers at once

`fetch` can be used for requesting data for several tickers at once. In this case pandas.Panel (instead of pandas.DataFrame) will be returned.

	res = DWE.fetch(['@AAPL','U:MMM'], fields=['P','MV','VO','PH'], date_from='2000-05-03')
	print res['MV'].head()
	
For convenience major and minor axes of panel are swapped, so the result is mimicing pandas method for [fetching data from Yahoo! Finance](http://pandas.pydata.org/pandas-docs/dev/remote_data.html#yahoo-finance). Panel can be sliced to get the data for each ticker:

	df = res.minor_xs('@AAPL')
	print df.head()
	
As discussed below, it may be convenient to set up property `raise_on_error` to `False` when fetching data of several tickers. In this case if one or several tickers are misspecified, error will no be raised, but the missing data will be replaced with NaNs:

	DWE.raise_on_error = False
	res = DWE.fetch(['@AAPL','U:MMM','xxxxxx','S&PCOMP'], fields=['P','MV','VO','PH'], date_from='2000-05-03')
	
	print res['MV'].head()
	print res['P'].head()
	
Please note, that in the last example the closing price (`P`) for `S&PCOMP` ticker was not retrieved. Due to Thomson Reuters Datastream mnenomics, field `P` is not available for indexes and field `PI` should be used instead.

### Constituents list for indices

PyDatastream also has an interface for retrieving list of constituents of indices:

	(res, status) = DWE.get_constituents('S&PCOMP')
	print res.ix[0]

As an option, the list for a specific date can be requested as well:

	(res, status) = DWE.get_constituents('S&PCOMP', '1-sept-2013')

## Advanced use

### Using custom requests

The module has a general-purpose function `request` that can be used for fetching data with custom requests. This function returns raw data in format of `suds` package. Data can be used directly or parsed later with the `parse_record` method:

	raw = DWE.request('@AAPL~=P,MV,VO,PH~2013-01-01~D')
	print raw['StatusType']
	print raw['StatusCode']

	data = DWE.extract_data(raw)
	print data['CCY']
	print data['MV']

Information about mnemonics and syntax of the request string can be found in [Thomson Financial Network](http://dtg.tfn.com/data/DataStream.html).

### Some useful tips with the Datastream syntax

Examples are taken from [Thomson Financial Network](http://dtg.tfn.com/data/DataStream.html) and [description of rDatastream package](https://github.com/fcocquemas/rdatastream).

#### Get performance information of a particular stock

	res = DWE.fetch('@AAPL~PERF', date_from='2011-09-01')
	print res.head()

#### Get some reference information on a security with `"~XREF"`, including ISIN, industry, etc.

	res = DWE.request('U:IBM~XREF')
	print DWE.extract_data(res)
    
#### Get some static items like NAME, ISIN with `"~REP"`

	res = DWE.request('U:IBM~=NAME,ISIN~REP')
	print DWE.extract_data(res)
	
#### Convert the currency e.g. to Euro with `"~~EUR"`

    res = DWE.fetch('U:IBM(P)~~EUR', date_from='2013-09-01')
    print res.head()
    
#### Calculate moving average on 20 days

	res = DWE.fetch('MAV#(U:IBM,20D)', date_from='2013-09-01')
	print res.head()

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

`raise_on_error` can be useful for requests that contain several tickers. In this case data fields and/or tickers that can not be fetched will be replaced with NaNs in resulting pandas.Panel.

For the debugging puroposes, Datastream class has `show_request` property, which, if set to `True`, makes standard methods to output the text string with request:

	DWE.show_request = True
	data = DWE.fetch('@AAPL', ['P','MV','VO',], date_from='2000-01-01')

Finally, method `status` could extract status info from the record with raw response:
	
	print DWE.status(res[1])

and `last_status` property always contains status of the last parsed record:

	print DWE.last_status

## Resources

It is recommended that you read the [Thomson Dataworks Enterprise User Guide](http://dataworks.thomson.com/Dataworks/Enterprise/1.0/documentation/user%20guide.pdf), especially section 4.1.2 on client design. It gives reasonable guidelines for not overloading the servers with too intensive requests.

For building custom Datastream requests, useful guidelines are given on this somewhat old [Thomson Financial Network](http://dtg.tfn.com/data/DataStream.html) webpage.

If you have access codes for the Datastream Extranet, you can use the [Datastream Navigator](http://product.datastream.com/navigator/) to look up codes and data types.

## Licence

PyDatastream is released under the MIT licence.