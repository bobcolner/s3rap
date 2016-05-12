from setuptools import find_packages, setup

VERSION = '0.3'

setup(
    name = 's3rap',
    packages = find_packages(),
    version = VERSION,
    platforms=['any'],
    description = 'AWS S3 convenience functions based on boto3.',
    author = 'Bob Colner',
    author_email = 'bcolner@gmail.com',
    url = 'https://github.com/bobcolner/s3rap', 
    download_url = 'https://github.com/bobcolner/s3rap/tarball/{0}'.format(VERSION),
    keywords = ['aws','s3','cloud'], # arbitrary keywords
    license = 'MIT',
    classifiers = [ # See: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Internet :: WWW/HTTP',
        # Pick your license as you wish (should match 'license' above)
        'License :: OSI Approved :: MIT License',
        # Specify the Python versions you support here.
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        ],
    install_requires = [ i.strip() for i in open("requirements.txt").readlines() ]
)
