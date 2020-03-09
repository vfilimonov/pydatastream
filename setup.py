from distutils.core import setup

DESCRIPTION = ('Python interface to the Refinitiv Datastream (former Thomson '
               'Reuters Datastream) API via Datastream Web Services (DSWS)')

# Long description to be published in PyPi
LONG_DESCRIPTION = """
**PyDatastream** is a Python interface to the Refinitiv Datastream (former Thomson
Reuters Datastream) API via Datastream Web Services (DSWS) (non free),
with some convenience functions. This package requires valid credentials for this
API.

For the documentation please refer to README.md inside the package or on the
GitHub (https://github.com/vfilimonov/pydatastream/blob/master/README.md).
"""

_URL = 'http://github.com/vfilimonov/pydatastream'

__version__ = __author__ = __email__ = None  # will be extracted from _version.py
exec(open('pydatastream/_version.py').read())  # defines __version__ pylint: disable=W0122

setup(name='PyDatastream',
      version=__version__,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      url=_URL,
      download_url=_URL + '/archive/v' + __version__ + '.zip',
      author=__author__,
      author_email=__email__,
      license='MIT License',
      packages=['pydatastream'],
      install_requires=['requests'],
      extras_require={
          'pandas':  ['pandas'],
          },
      classifiers=['Programming Language :: Python :: 3'],
      )
