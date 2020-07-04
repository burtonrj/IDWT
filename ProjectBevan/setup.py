from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='ProjectBevan',
    version='0.0.1',
    packages=['sql', 'nosql', 'tests'],
    package_dir={'': 'ProjectBevan'},
    install_requires=requirements,
    url='https://github.com/burtonrj/IDWT',
    license='MIT',
    author='Ross Burton',
    author_email='burtonrj@cardiff.ac.uk',
    description='ETL pipeline for the In Data We Trust project'
)
