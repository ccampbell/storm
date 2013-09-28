from setuptools import setup
from storm import version

# magic
setup(
    name='tornado-storm',
    version=version,
    description='A simple ORM for Tornado',
    author='Craig Campbell',
    author_email='iamcraigcampbell@gmail.com',
    url='https://github.com/ccampbell/storm',
    download_url='https://github.com/ccampbell/storm/archive/0.1.0.zip#egg=tornado-storm-0.1.0',
    license='MIT',
    install_requires=['tornado >= 3.1', 'motor == 0.1.1'],
    py_modules=['storm'],
    platforms=["any"]
)
