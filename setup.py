"""Setup module for eider, using setuptools."""

from setuptools import setup

# Extract version string from module without importing it
__version__ = 'unknown'
with open('eider.py') as fin:
    for line in fin:
        if line.startswith('__version__ = '):
            exec(line)
            break

with open('README.rst') as fin:
    readme = fin.read()

setup(
    name='eider',
    version=__version__,
    description=(
        'An object-oriented, asynchronous, late-binding, web-first, polyglot '
        'RPC protocol'),
    long_description=readme,
    url='https://github.com/eider-rpc/eider-py',
    author='Bart Robinson',
    author_email='bart@bartrobinson.com',
    license='Apache License 2.0',
    platforms='any',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System :: Distributed Computing'
    ],
    py_modules=['eider'],
    install_requires=[],
    zip_safe=False
)
