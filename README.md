[![Build Status](https://travis-ci.org/DigitalPebble/behemoth.svg?branch=master)](https://travis-ci.org/DigitalPebble/behemoth)

Behemoth is an open source platform for large scale document processing based on Apache Hadoop.

It consists of a simple annotation-based implementation of a document and a number of modules operating on these documents.
One of the main aspects of Behemoth is to simplify the deployment of document analysers on a large scale but also to provide reusable modules for :
- ingesting from common data sources (Warc, Nutch, etc...)
- text processing (Tika, UIMA, GATE, Language Identification)
- generating output for external tools (SOLR, Mahout)

Its modular architecture simplifies the development of custom annotators based on MapReduce.

Note that Behemoth does not implement any NLP or Machine Learning components as such but serves as a 'large-scale glueware' for existing resources. Being Hadoop-based, it benefits from all its features, namely scalability, fault-tolerance and most notably the back up of a thriving open source community. 

WIKI : https://github.com/DigitalPebble/behemoth/wiki

Mailing list : http://groups.google.com/group/digitalpebble 
