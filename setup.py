from distutils.core import setup

setup(name = 'PyDatastream',
      version = '0.1.1',
      description = 'Python interface to the Thomson Dataworks Enterprise SOAP API.',
      url = 'http://github.com/vfilimonov/pydatastream',
      author = 'Vladimir Filimonov',
      author_email = 'vfilimonov@ethz.ch',
      license = 'MIT License',
      packages = ['pydatastream'],
      install_requires = ['suds', 'pandas']
     )
