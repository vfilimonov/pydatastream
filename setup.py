from distutils.core import setup
import sys

install_requires_list = ['pandas', 'requests']

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

exec(open('pydatastream/_version.py').read())

setup(name='PyDatastream',
      version=__version__,
      description='Python interface to the Refinitiv Datastream (former Thomson Reuters Datastream) API via Datastream Web Services (DSWS)',
      long_description=LONG_DESCRIPTION,
      url=_URL,
      download_url=_URL + '/archive/v' + __version__ + '.zip',
      author='Vladimir Filimonov',
      author_email='vladimir.a.filimonov@gmail.com',
      license='MIT License',
      packages=['pydatastream'],
      install_requires=install_requires_list,
      classifiers=['Programming Language :: Python :: 2',
                   'Programming Language :: Python :: 3', ]
      )
