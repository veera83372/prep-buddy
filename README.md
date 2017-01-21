## Prep Buddy 
[![Build Status](https://travis-ci.org/data-commons/prep-buddy.svg?branch=master)](https://travis-ci.org/data-commons/prep-buddy)
#### Data Preparation Library for Spark

A Scala / Java / Python library for cleaning, transforming and executing other preparation tasks for large datasets on Apache Spark.

It is currently maintained by a team of developers from [ThoughtWorks](http://www.thoughtworks.com).

Post questions and comments to the Google group, or email them directly to data-commons-toolchain@googlegroups.com

Docs are available at http://data-commons.github.io/prep-buddy
Or check out the [Scaladocs](http://data-commons.github.io/prep-buddy/scaladocs).

Our aim is to provide a set of algorithms for cleaning and transforming very large data sets,                
inspired by predecessors such as Open Refine, Pandas and Scikit-learn packages.

#### Important links

- Official source code repo: https://github.com/data-commons/prep-buddy
- Scala docs (development version): http://data-commons.github.io/prep-buddy/scaladocs
- Download releases: [Latest Release](https://github.com/data-commons/prep-buddy/releases/latest)
- Issue tracker: [Github](https://github.com/data-commons/prep-buddy/issues)
- Mailing list: data-commons-toolchain@googlegroups.com
- Slack channel: Coming soon!

#### Usage!
To use this library, add a maven dependency to datacommons in your project:
```
<dependency>
    <groupId>com.thoughtworks.datacommons</groupId>
    <artifactId>prep-buddy</artifactId>
    <version>0.5.1</version>
</dependency>
```
For other build tools check on [Maven Repositry](https://mvnrepository.com/artifact/com.thoughtworks.datacommons/prep-buddy/0.5.0)

##Python

If you don't have pip. Intsall [pip](https://pip.pypa.io/en/stable/installing/).
```
pip install prep-buddy
```

For using pyspark on command-line
Download the [Jar](https://github.com/data-commons/prep-buddy/releases/tag/v-0.5.1).
```
pyspark --jars [PATH-TO-JAR]
```
```
spark-submit --driver-class-path [PATH-TO-JAR] [Your python file.]
```

This library is currently built for Spark 1.6.x, but is also compatible with 1.4.x. 
#### Dependencies
The library depends on a few other libraries.
- Apache Commons Math for general math and statistics functionality.
- Apache Spark for all the distributed computation capabilities.
- Open CSS for parsing the files.
#### Download

- Stable [0.5.1(Beta)](https://github.com/data-commons/prep-buddy/releases/tag/v-0.5.1).

#### Documentation Wiki
- http://data-commons.github.io/prep-buddy

#### Contributing
- Create a pull request.
