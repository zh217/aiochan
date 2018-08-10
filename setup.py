from setuptools import setup, find_packages

setup(
    name='aiochan',
    version='0.1.1',
    packages=find_packages(),
    platforms='any',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache License 2.0',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    license='Apache',
    python_requires='>=3.5.3',
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'pytest-asyncio'],
    author='Ziyang Hu',
    author_email='hu.ziyang@cantab.net',
    description='CSP-style concurrency for Python',
    url='https://github.com/zh217/aiochan',
    project_urls={}
)
