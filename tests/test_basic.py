""" Tests for basic pydatastream functionality

    (c) Vladimir Filimonov, 2019
"""
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
    assert _parse_dates(
        '/Date(1565817068486)/') == pd.Timestamp('2019-08-14 21:11:08.486000')
    assert _parse_dates(
        '/Date(1565568000000+0000)/') == pd.Timestamp('2019-08-12 00:00:00')

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
def test_construct_request_1():
    """ Construct request for series """
    req = {'Instrument': {'Value': '@AAPL', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
           'Date': {'Start': '2008-01-01', 'End': '2009-01-01', 'Frequency': '', 'Kind': 1},
           'DataTypes': []}
    DS = empty_datastream()
    assert DS.construct_request('@AAPL', date_from='2008', date_to='2009') == req


###############################################################################
def test_construct_request_2():
    """ Construct request for series """
    req = {'Instrument': {'Value': '@AAPL', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
           'Date': {'Start': 'BDATE', 'End': '', 'Frequency': '', 'Kind': 1},
           'DataTypes': [{'Value': 'P', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
                         {'Value': 'MV', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
                         {'Value': 'VO', 'Properties': [{'Key': 'ReturnName', 'Value': True}]}]}
    DS = empty_datastream()
    assert DS.construct_request('@AAPL', ['P', 'MV', 'VO'], date_from='bdate') == req


###############################################################################
def test_construct_request_3():
    """ Construct static request """
    req = {'Instrument': {'Value': 'D:BAS,D:BASX,HN:BAS',
                          'Properties': [{'Key': 'IsList', 'Value': True},
                                         {'Key': 'ReturnName', 'Value': True}]},
           'Date': {'Start': '', 'End': '', 'Frequency': '', 'Kind': 0},
           'DataTypes': [{'Value': 'ISIN', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
                         {'Value': 'ISINID', 'Properties': [{'Key': 'ReturnName', 'Value': True}]},
                         {'Value': 'NAME', 'Properties': [{'Key': 'ReturnName', 'Value': True}]}]}
    DS = empty_datastream()
    assert DS.construct_request(['D:BAS','D:BASX','HN:BAS'], ['ISIN', 'ISINID', 'NAME'], static=True) == req
