from setuptools import setup, find_packages
from storm import version

# magic
setup(
    name='tornado-storm',
    version=version,
    description='A simple ORM for Tornado',
    author='Craig Campbell',
    author_email='iamcraigcampbell@gmail.com',
    url='https://github.com/ccampbell/storm',
    download_url='https://github.com/ccampbell/storm/archive/%s.zip#egg=tornado-storm-%s' % (version, version),
    license='MIT',
    install_requires=['tornado >= 4.0', 'tornado_mysql >= 0.5', 'motor >= 0.1.1'],
    packages=find_packages(),
    py_modules=['storm'],
    platforms=["any"]
)
