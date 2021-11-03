import setuptools

setuptools.setup(name='whstudy',
version='0.1',
description='API for easier use of mongo database with world bank data and world happiness report data.',
url='#',
author='Nejc Hirci',
install_requires=['pandas', 'pymongo', 'wbgapi'],
author_email='',
packages=setuptools.find_packages(),
zip_safe=False)