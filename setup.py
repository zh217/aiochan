from setuptools import setup, find_packages

setup(
    name='aiochan',
    version='0.2.2',
    packages=find_packages(),
    platforms='any',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
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
    project_urls={},
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
)
