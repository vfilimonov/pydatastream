from distutils.core import setup
import sys

if sys.version_info.major < 3:
    install_requires_list = ['suds', 'pandas']
else:
    install_requires_list = ['suds-py3', 'pandas']

# Long description to be published in PyPi
LONG_DESCRIPTION = """
**PyDatastream** is a Python interface to the **Thomson Dataworks Enterprise
(DWE)** SOAP API (non free), with some convenience functions for retrieving
Datastream data specifically. This package requires valid credentials for this
API.

For the documentation please refer to README.md inside the package or on the
GitHub (https://github.com/vfilimonov/pydatastream/blob/master/README.md).
"""

setup(name='PyDatastream',
      version='0.4.3',
      description='Python interface to the Thomson Reuters Dataworks Enterprise (Datastream) API',
      long_description=LONG_DESCRIPTION,
      url='http://github.com/vfilimonov/pydatastream',
      download_url='https://github.com/vfilimonov/pydatastream/archive/v0.4.1.zip',
      author='Vladimir Filimonov',
      author_email='vladimir.a.filimonov@gmail.com',
      license='MIT License',
      packages=['pydatastream'],
      install_requires=install_requires_list,
      classifiers=['Programming Language :: Python :: 2',
                   'Programming Language :: Python :: 3', ]
      )
