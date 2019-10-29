""" Tests for basic pydatastream functionality

    (c) Vladimir Filimonov, 2019
"""
# pylint: disable=C0103,C0301
import warnings
import pytest
import pydatastream as pds


###############################################################################
def empty_datastream():
    """ Create an instance of datastream """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return pds.Datastream(None, None)


###############################################################################
###############################################################################
def test_create():
    """ Test init without username """
    empty_datastream()


###############################################################################
def test_create_wrong_credentials():
    """ Test init with wrong credentials - should raise an exception """
    with pytest.raises(pds.DatastreamException):
        assert pds.Datastream("USERNAME", "PASSWORD")


###############################################################################
def test_convert_date():
    """ Test _convert_date() """
    from pydatastream.pydatastream import _convert_date
    assert _convert_date('5 January 2019') == '2019-01-05'
    assert _convert_date('04/02/2017') == '2017-04-02'
    assert _convert_date('2019') == '2019-01-01'
    assert _convert_date('2019-04') == '2019-04-01'
    assert _convert_date(None) == ''
    assert _convert_date('bdate') == 'BDATE'


###############################################################################
def test_parse_dates():
    """ Test _parse_dates() """
    from pydatastream.pydatastream import _parse_dates
    import pandas as pd
    assert _parse_dates('/Date(1565817068486)/') == pd.Timestamp('2019-08-14 21:11:08.486000')
    assert _parse_dates('/Date(1565568000000+0000)/') == pd.Timestamp('2019-08-12 00:00:00')

    # Array of dates
    dates = ['/Date(1217548800000+0000)/', '/Date(1217808000000+0000)/',
             '/Date(1217894400000+0000)/', '/Date(1217980800000+0000)/',
             '/Date(1218067200000+0000)/', '/Date(1218153600000+0000)/',
             '/Date(1218412800000+0000)/', '/Date(1218499200000+0000)/',
             '/Date(1218585600000+0000)/', '/Date(1218672000000+0000)/',
             '/Date(1218758400000+0000)/', '/Date(1219017600000+0000)/',
             '/Date(1219104000000+0000)/', '/Date(1219190400000+0000)/',
             '/Date(1219276800000+0000)/', '/Date(1219363200000+0000)/',
             '/Date(1219622400000+0000)/', '/Date(1219708800000+0000)/',
             '/Date(1219795200000+0000)/', '/Date(1219881600000+0000)/',
             '/Date(1219968000000+0000)/', '/Date(1220227200000+0000)/']
    res = ['2008-08-01', '2008-08-04', '2008-08-05', '2008-08-06', '2008-08-07',
           '2008-08-08', '2008-08-11', '2008-08-12', '2008-08-13', '2008-08-14',
           '2008-08-15', '2008-08-18', '2008-08-19', '2008-08-20', '2008-08-21',
           '2008-08-22', '2008-08-25', '2008-08-26', '2008-08-27', '2008-08-28',
           '2008-08-29', '2008-09-01']
    assert pd.np.all(_parse_dates(dates) == pd.np.array(res, dtype='datetime64[ns]'))


###############################################################################
# Construct requests
###############################################################################
def test_construct_request_series_1():
    """ Construct request for series """
    req = {'Instrument': {'Value': '@AAPL', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
           'Date': {'Start': '2008-01-01', 'End': '2009-01-01', 'Frequency': '', 'Kind': 1},
           'DataTypes': []}
    DS = empty_datastream()
    assert DS.construct_request('@AAPL', date_from='2008', date_to='2009') == req


###############################################################################
def test_construct_request_series_2():
    """ Construct request for series """
    req = {'Instrument': {'Value': '@AAPL', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
           'Date': {'Start': 'BDATE', 'End': '', 'Frequency': '', 'Kind': 1},
           'DataTypes': [{'Value': 'P', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
                         {'Value': 'MV', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
                         {'Value': 'VO', 'Properties': [{'Key': 'ReturnName', 'Value': True}]}]}
    DS = empty_datastream()
    assert DS.construct_request('@AAPL', ['P', 'MV', 'VO'], date_from='bdate') == req


###############################################################################
def test_construct_request_static_1():
    """ Construct static request """
    req = {'Instrument': {'Value': 'D:BAS,D:BASX,HN:BAS',
                          'Properties': [{'Key': 'IsList', 'Value': True},
                                         {'Key': 'ReturnName', 'Value': True}]},
           'Date': {'Start': '', 'End': '', 'Frequency': '', 'Kind': 0},
           'DataTypes': [{'Value': 'ISIN', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
                         {'Value': 'ISINID', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
                         {'Value': 'NAME', 'Properties': [{'Key': 'ReturnName', 'Value': True}]}]}
    DS = empty_datastream()
    assert DS.construct_request(['D:BAS', 'D:BASX', 'HN:BAS'], ['ISIN', 'ISINID', 'NAME'], static=True) == req


###############################################################################
# Parse responses
###############################################################################
def test_parse_response_static_1():
    """ Parse static request """
    import pandas as pd
    from io import StringIO

    s = (',,ISIN,ISINID,NAME\nD:BAS,2019-09-27,DE000BASF111,P,BASF\n'
         'D:BASX,2019-09-27,DE000BASF111,S,BASF (XET)\n'
         'HN:BAS,2019-09-27,DE000BASF111,S,BASF (BUD)\n')
    df = pd.read_csv(StringIO(s), index_col=[0, 1], parse_dates=[1])

    res = {'AdditionalResponses': None,
           'DataTypeNames': [{'Key': 'ISIN', 'Value': 'ISIN CODE'},
                             {'Key': 'ISINID', 'Value': 'QUOTE INDICATOR'},
                             {'Key': 'NAME', 'Value': 'NAME'}],
           'DataTypeValues': [{'DataType': 'ISIN',
                               'SymbolValues': [{'Currency': 'E ', 'Symbol': 'D:BAS', 'Type': 6, 'Value': 'DE000BASF111'},
                                                {'Currency': 'E ', 'Symbol': 'D:BASX', 'Type': 6, 'Value': 'DE000BASF111'},
                                                {'Currency': 'HF', 'Symbol': 'HN:BAS', 'Type': 6, 'Value': 'DE000BASF111'}]},
                              {'DataType': 'ISINID',
                               'SymbolValues': [{'Currency': 'E ', 'Symbol': 'D:BAS', 'Type': 6, 'Value': 'P'},
                                                {'Currency': 'E ', 'Symbol': 'D:BASX', 'Type': 6, 'Value': 'S'},
                                                {'Currency': 'HF', 'Symbol': 'HN:BAS', 'Type': 6, 'Value': 'S'}]},
                              {'DataType': 'NAME',
                               'SymbolValues': [{'Currency': 'E ', 'Symbol': 'D:BAS', 'Type': 6, 'Value': 'BASF'},
                                                {'Currency': 'E ', 'Symbol': 'D:BASX', 'Type': 6, 'Value': 'BASF (XET)'},
                                                {'Currency': 'HF', 'Symbol': 'HN:BAS', 'Type': 6, 'Value': 'BASF (BUD)'}]}],
           'Dates': ['/Date(1569542400000+0000)/'],
           'SymbolNames': [{'Key': 'D:BAS', 'Value': 'D:BAS'},
                           {'Key': 'D:BASX', 'Value': 'D:BASX'},
                           {'Key': 'HN:BAS', 'Value': 'HN:BAS'}], 'Tag': None}
    res = {'DataResponse': res, 'Properties': None}

    DS = empty_datastream()
    assert DS.parse_response(res).equals(df)


###############################################################################
def test_parse_response_series_1():
    """ Parse response as a series """
    import pandas as pd
    from io import StringIO

    s = (',,P,MV,VO\n@AAPL,1999-12-31,3.6719,16540.47,40952.8\n@AAPL,2000-01-03,3.9978,18008.5,133932.3\n'
         '@AAPL,2000-01-04,3.6607,16490.2,127909.5\n@AAPL,2000-01-05,3.7143,16760.53,194426.3\n'
         '@AAPL,2000-01-06,3.3929,15310.1,191981.9\n@AAPL,2000-01-07,3.5536,16035.32,115180.7\n'
         '@AAPL,2000-01-10,3.4911,15753.29,126265.9\n')
    df = pd.read_csv(StringIO(s), index_col=[0, 1], parse_dates=[1])

    res = {'AdditionalResponses': [{'Key': 'Frequency', 'Value': 'D'}],
           'DataTypeNames': None,
           'DataTypeValues': [{'DataType': 'P',
                               'SymbolValues': [{'Currency': 'U$', 'Symbol': '@AAPL', 'Type': 10,
                                                 'Value': [3.6719, 3.9978, 3.6607, 3.7143, 3.3929, 3.5536, 3.4911]}]},
                              {'DataType': 'MV',
                               'SymbolValues': [{'Currency': 'U$', 'Symbol': '@AAPL', 'Type': 10,
                                                 'Value': [16540.47, 18008.5, 16490.2, 16760.53, 15310.1, 16035.32, 15753.29]}]},
                              {'DataType': 'VO',
                               'SymbolValues': [{'Currency': 'U$', 'Symbol': '@AAPL', 'Type': 10,
                                                 'Value': [40952.8, 133932.3, 127909.5, 194426.3, 191981.9, 115180.7, 126265.9]}]}],
           'Dates': ['/Date(946598400000+0000)/', '/Date(946857600000+0000)/',
                     '/Date(946944000000+0000)/', '/Date(947030400000+0000)/',
                     '/Date(947116800000+0000)/', '/Date(947203200000+0000)/',
                     '/Date(947462400000+0000)/'],
           'SymbolNames': None, 'Tag': None}
    res = {'DataResponse': res, 'Properties': None}

    DS = empty_datastream()
    assert DS.parse_response(res).equals(df)
