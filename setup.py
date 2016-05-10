from distutils.core import setup

setup(
    name = 's3rap',
    packages = ['s3rap'], # this must be the same as the name above
    version = '0.1',
    description = 'AWS S3 convenience functions based on boto3.',
    author = 'Bob Colner',
    author_email = 'bcolner@gmail.com',
    url = 'https://github.com/bobcolner/s3rap', 
    download_url = 'https://github.com/bobcolner/s3rap/tarball/0.1',
    keywords = ['aws','s3','cloud'], # arbitrary keywords
    license = 'MIT',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers = [
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        # Pick your license as you wish (should match 'license' above)
        'License :: OSI Approved :: MIT License',
        # Specify the Python versions you support here.
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        ],
    install_requires = ['boto3']
)
