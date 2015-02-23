from distutils.core import setup

setup(name = 'PyDatastream',
      version = '0.2.1',
      description = 'Python interface to the Thomson Reuters Dataworks Enterprise (Datastream) API',
      url = 'http://github.com/vfilimonov/pydatastream',
      download_url = 'https://github.com/vfilimonov/pydatastream/archive/v0.2.1.zip', 
      author = 'Vladimir Filimonov',
      author_email = 'vfilimonov@ethz.ch',
      license = 'MIT License',
      packages = ['pydatastream'],
      install_requires = ['suds', 'pandas']
     )
