from setuptools import setup, find_packages

setup(
    name='aiochan',
    version='0.1.0',
    packages=find_packages(),
    python_requires='>=3.6',
    setup_requires=['pytest-runner'],
    tests_requires=['pytest', 'pytest-asyncio'],
    author='Ziyang Hu',
    author_email='hu.ziyang@cantab.net',
    description='CSP-style concurrency for Python',
    url='https://github.com/zh217/aiochan',
    project_urls={}
)
