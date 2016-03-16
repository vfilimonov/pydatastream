from distutils.core import setup
import sys

if sys.version_info.major < 3:
    install_requires_list = ['suds', 'pandas']
else:
    install_requires_list = ['suds-py3', 'pandas']

setup(name = 'PyDatastream',
      version = '0.4.0',
      description = 'Python interface to the Thomson Reuters Dataworks Enterprise (Datastream) API',
      url = 'http://github.com/vfilimonov/pydatastream',
      download_url = 'https://github.com/vfilimonov/pydatastream/archive/v0.4.0.zip',
      author = 'Vladimir Filimonov',
      author_email = 'vladimir.a.filimonov@gmail.com',
      license = 'MIT License',
      packages = ['pydatastream'],
      install_requires = install_requires_list,
      classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ]
     )
