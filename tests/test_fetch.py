""" Tests for fetching the data using pydatastream

    Note: Proper credentials should be stored in environment variables:
          DSWS_USER and DSWS_PASS

    (c) Vladimir Filimonov, 2019
"""
import os
import pytest
import pydatastream as pds


###############################################################################
###############################################################################
def has_credentials():
    """ Credentials are in the environment variables """
    return ('DSWS_USER' in os.environ) and ('DSWS_PASS' in os.environ)


def init_datastream():
    """ Create an instance of datastream """
    return pds.Datastream(os.environ['DSWS_USER'], os.environ['DSWS_PASS'])


###############################################################################
# Tests will be skipped if credentials are not passed
###############################################################################
@pytest.mark.skipif(not has_credentials(),
                    reason="credentials are not passed")
def test_create_with_login():
    """ Test init with credentials """
    init_datastream()
