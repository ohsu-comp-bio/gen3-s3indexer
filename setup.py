from distutils.core import setup

setup(
    name='s3indexer',
    version='0.1dev',
    packages=['s3indexer',],
    description='Reads blank records from indexd, triggers s3index.',
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.md').read(),
)
