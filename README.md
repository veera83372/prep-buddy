## Prep Buddy 
[![Build Status](https://travis-ci.org/data-commons/prep-buddy.svg?branch=master)](https://travis-ci.org/data-commons/prep-buddy)
#### Data Preparation Library for Spark

A Scala / Java / Python library for cleaning, transforming and executing other preparation tasks for large datasets on Apache Spark.

It is currently maintained by a team of volunteers.

Post questions and comments to the Google group, or email them directly to data-commons-toolchain@googlegroups.com

Docs are available at http://data-commons.github.io/prep-buddy
Or check out the Scaladoc, Javadoc, or Python doc.

Our aim is to provide a set of algorithms for cleaning and transforming very large data sets, inspired by predecessors such as Open Refine, Pandas and Scikit-learn packages.

#### Important links

- Official source code repo: https://github.com/data-commons/prep-buddy
- HTML documentation (stable release): 
- HTML documentation (development version): 
- Download releases: 
- Issue tracker: https://github.com/data-commons/prep-buddy/issues
- Mailing list: data-commons-toolchain@googlegroups.com
- Slack channel: 

#### Usage
To use this library, add a dependency to spark-metrics in your project:
<dependency>
    <groupId>com.groupon.dse</groupId>
    <artifactId>spark-metrics</artifactId>
    <version>1.0</version>
</dependency>
This library is currently built for Spark 1.5.2, but is also compatible with 1.4.1. This is important to note because this library uses Spark's internal APIs, and compatibility with other major Spark versions has not been fully tested.

#### Dependencies
The library depends on a few other Java libraries.

- Javallier - A Java library for Paillier partially homomorphic encryption based on python-paillier.
- Apache Commons Math for general math and statistics functionality.
- Apache Spark for all the distributed computation capabilities.

#### Download

- Latest Stable 0.5. Coming soon!

#### Documentation Wiki
- http://data-commons.github.io/prep-buddy

#### Contributing
- Coming Soon!

