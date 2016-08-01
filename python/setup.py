from setuptools import setup, find_packages

setup(
    name='pyprepbuddy',
    description='A library for data preparation at scale with Apache Spark',
    author='data-commons',
    author_email='data-commons-toolchain@googlegroups.com',
    url='https://github.com/data-commons/prep-buddy',
    version="0.3.0",
    packages=find_packages(),
    include_package_data=True,
    classifiers=[],
    keywords=['prepbuddy', 'pyprepbuddy', 'data', 'preparation'],
    install_requires=[
        'numpy >= 1.11.1'
    ],
    test_requires=[
        'nose == 1.3.7'
    ],
    license='Apache 2.0'
)
