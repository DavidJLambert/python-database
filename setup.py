""" setup.py
https://github.com/DavidJLambert/Python-Universal-DB-Client
Version: 0.2.2
Author: David J. Lambert
Date: May 2, 2019
"""

from distutils.core import setup

setup(
    name='universalClient',
    version='0.2.2',
    description='Python Universal Database Client',
    long_description=open('README.rst').read(),
    long_description_content_type='text/x-rst',
    url='https://github.com/DavidJLambert/Python-Universal-DB-Client',
    author='David J. Lambert',
    author_email='David5Lambert7@gmail.com',
    license='MIT License',
    py_modules=["universalClient"],
    classifiers=[
        'Topic :: Multimedia :: Video',
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Topic :: Database :: Front-Ends',
    ],
)
