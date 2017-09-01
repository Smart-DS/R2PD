from distutils.core import setup

setup(
    name='R2PD',
    version='0.1.0',
    author='Michael Rossol',
    author_email='michael.rossol@nrel.gov',
    packages=['R2PD', ],
    scripts=['bin/r2pd.py', ],
    url='https://github.com/Smart-DS/R2PD',
    description='Power system modeler-friendly tool for downloading and \
formatting wind and solar weather, power and forecast data.',
)
