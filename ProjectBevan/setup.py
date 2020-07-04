from setuptools import setup

setup(
    name='ProjectBevan',
    version='0.0.1',
    packages=['sql', 'nosql', 'tests'],
    package_dir={'': 'ProjectBevan'},
    url='https://github.com/burtonrj/IDWT',
    license='MIT',
    author='Ross Burton',
    author_email='burtonrj@cardiff.ac.uk',
    description='ETL pipeline for the In Data We Trust project'
)
