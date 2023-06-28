from setuptools import setup

setup(
    name='pushcap',
    version='1.01',
    description='A REDCap uploader for various assessments and forms.',
    author='Aaron Piccirilli',
    author_email='picc@stanford.edu',
    packages=['pushcap'],
    install_requires=['requests', 'python-dateutil']
)
