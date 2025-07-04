# Flame - Distributed Data Processing Framework

Flame is a distributed data processing framework written in Java, similar to Apache Spark. It provides a resilient distributed dataset (RDD) abstraction for processing large-scale data across a cluster of machines.

## Overview

Flame consists of three main components:

1. **KVS (Key-Value Store)** - A distributed key-value storage system that serves as the underlying storage layer
2. **Flame Engine** - The distributed computing engine that provides RDD operations and data processing capabilities
3. **Web Server** - HTTP server for job submission and management

## Architecture

- **Coordinator Nodes** - Manage cluster state and coordinate job execution
- **Worker Nodes** - Execute data processing tasks and store data partitions
- **RDD Operations** - Transformations (map, filter, flatMap) and actions (collect, count, saveAsTable)
- **Distributed Storage** - Data is automatically partitioned and distributed across worker nodes

## Key Features

- **Resilient Distributed Datasets (RDDs)** - Fault-tolerant collections of data that can be processed in parallel
- **Lazy Evaluation** - Operations are only executed when actions are called
- **Fault Tolerance** - Automatic recovery from node failures
- **Scalability** - Horizontal scaling across multiple machines
- **Web-based Job Submission** - Submit and monitor jobs via HTTP interface

## Quick Start

### To run search backend
- `java -cp src flame.app.App <kvs coordinator address>`

### To use search (demo):
- Make sure crawler and pagerank have been run 
- Make sure tfidf.jar exists in root folder (can be built from jobs/TFIDF.java)
- Run TFIDF Test with arg `tfidf` to generate pt-tf and pt-idf tables
- Run search/Search.java with argument of KVS coordinator: `java -cp src flame.search.Search localhost:8000`. This should take < 1s
- To modify the demo search string or results returned, amend Search.java and recompile. Default # of results returned is 10

## Project Structure

```
flame/
├── src/flame/           # Main source code
│   ├── flame/          # Core Flame engine (RDD operations)
│   ├── kvs/            # Key-Value Store implementation
│   ├── webserver/      # HTTP server for job submission
│   ├── jobs/           # Example data processing jobs
│   ├── search/         # Search functionality
│   ├── tools/          # Utility classes
│   └── app/            # Application entry points
├── lib/                # Compiled JAR files
├── frontend/           # Web interface
└── tests/              # Test JAR files
```

## Example Jobs

Flame includes several example data processing jobs:

- **Crawler** - Web crawler for collecting data from websites
- **PageRank** - Implementation of Google's PageRank algorithm
- **Indexer** - Text indexing for search functionality
- **TFIDF** - Term Frequency-Inverse Document Frequency for text analysis
- **MinimalTestJob** - Simple example job for testing

## Building and Deployment

### Compile & Jar: Webserver, KVS, and Flame
## Webserver Compilation and Jar
```bash
  javac --source-path src -d bin src/flame/webserver/*.java
```
```bash
  jar cvf lib/webserver.jar -C bin flame/webserver -C bin flame/tools
```

## KVS Compilation and JAR
```bash
  javac --source-path src -d bin -cp lib/webserver.jar src/flame/kvs/*.java
```
```bash
  jar cvf lib/kvs.jar -C bin flame/kvs -C bin flame/generic
```

## Flame Compilation and JAR
```bash
  javac --source-path src -d bin -classpath lib/webserver.jar:lib/kvs.jar src/flame/flame/*.java
```
```bash
  jar cvf lib/flame.jar -C bin flame/flame
```

## Compile & Jar: Crawler, PageRank, Indexer, TFIDF
```bash
  javac --source-path src -d bin -classpath lib/webserver.jar:lib/kvs.jar:lib/flame.jar:lib/aws-java-sdk-s3-1.12.261.jar:lib/aws-java-sdk-core-1.12.261.jar:lib/aws-java-sdk-kms-1.12.261.jar:lib/jsoup-1.18.3.jar src/flame/jobs/*.java
```
```bash
  jar cvf crawler.jar -C bin flame/jobs/Crawler.class
```
```bash
  jar cvf pagerank.jar -C bin flame/jobs/PageRank.class
```
```bash
  jar cvf indexer.jar -C bin flame/jobs/Indexer.class
```
```bash
  jar cvf tfidf.jar -C bin flame/jobs/TFIDF.class
```
```bash
  jar cvf minimaltestjob.jar -C bin flame/jobs/MinimalTestJob.class
```
```bash
  jar cvf indexinitializer.jar -C bin flame/jobs/IndexInitializer.class
```
## Run KVS and Flame Commands
```bash
  java -cp lib/kvs.jar:lib/webserver.jar flame.kvs.Coordinator 8000
```
```bash
  java -cp lib/kvs.jar:lib/webserver.jar flame.kvs.Worker 8001 worker1 localhost:8000
```
```bash
  java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar flame.flame.Coordinator 9000 localhost:8000
```
```bash
  java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar flame.flame.Worker 9001 localhost:9000
```

## Running the System

### Local Development
For local development and testing, you can run the system on a single machine:

1. Start the KVS Coordinator: `java -cp lib/kvs.jar:lib/webserver.jar flame.kvs.Coordinator 8000`
2. Start a KVS Worker: `java -cp lib/kvs.jar:lib/webserver.jar flame.kvs.Worker 8001 worker1 localhost:8000`
3. Start the Flame Coordinator: `java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar flame.flame.Coordinator 9000 localhost:8000`
4. Start a Flame Worker: `java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar flame.flame.Worker 9001 localhost:9000`

### Production Deployment
For production deployment on AWS EC2 instances:

## KVS Coordinator
```bash
  scp -i flame.pem -r lib loadfroms3.jar crawler.jar pagerank.jar indexer.jar ubuntu@ec2-3-83-158-153.compute-1.amazonaws.com:/home/ubuntu/project/
```
```bash
  ssh -i flame.pem ubuntu@ec2-3-83-158-153.compute-1.amazonaws.com
```
```bash
  cd project/
```
```bash
  java -cp lib/kvs.jar:lib/webserver.jar flame.kvs.Coordinator 8000
```

## KVS Worker(s)
```bash
  scp -i flame.pem -r lib loadfroms3.jar crawler.jar pagerank.jar indexer.jar ubuntu@ec2-3-84-45-255.compute-1.amazonaws.com:/home/ubuntu/project/
```
```bash
  ssh -i flame.pem ubuntu@ec2-3-84-45-255.compute-1.amazonaws.com     
```
```bash
  scp -i flame.pem -r lib loadfroms3.jar crawler.jar pagerank.jar indexer.jar ubuntu@ec2-44-211-140-129.compute-1.amazonaws.com:/home/ubuntu/project/ 
```
```bash
  ssh -i flame.pem ubuntu@ec2-44-211-140-129.compute-1.amazonaws.com
```
```bash
  scp -i flame.pem -r lib loadfroms3.jar crawler.jar pagerank.jar indexer.jar ubuntu@ec2-18-212-2-158.compute-1.amazonaws.com:/home/ubuntu/project/
```
```bash
  ssh -i flame.pem ubuntu@ec2-18-212-2-158.compute-1.amazonaws.com
```
Then, for each:
```bash
  cd project
```
```bash
  java -cp lib/kvs.jar:lib/webserver.jar flame.kvs.Worker 8001 worker1 3.83.158.153:8000
```

## Flame Coordinator
```bash
  scp -i flame.pem -r lib loadfroms3.jar crawler.jar pagerank.jar indexer.jar ubuntu@ec2-52-90-29-134.compute-1.amazonaws.com:/home/ubuntu/project/
```
```bash
  ssh -i flame.pem ubuntu@ec2-52-90-29-134.compute-1.amazonaws.com
```
```bash
  cd project
```
```bash
  java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar flame.flame.Coordinator 9000 3.83.158.153:8000
```

## Flame Worker(s)
```bash
  scp -i flame.pem -r lib loadfroms3.jar crawler.jar pagerank.jar indexer.jar ubuntu@ec2-100-24-44-55.compute-1.amazonaws.com:/home/ubuntu/project/
```
```bash
  ssh -i flame.pem ubuntu@ec2-100-24-44-55.compute-1.amazonaws.com
```
```bash
  scp -i flame.pem -r lib loadfroms3.jar crawler.jar pagerank.jar indexer.jar ubuntu@ec2-3-84-4-49.compute-1.amazonaws.com:/home/ubuntu/project/
```
```bash
  ssh -i flame.pem ubuntu@ec2-3-84-4-49.compute-1.amazonaws.com
```
```bash
  scp -i flame.pem -r lib loadfroms3.jar crawler.jar pagerank.jar indexer.jar ubuntu@ec2-44-211-205-144.compute-1.amazonaws.com:/home/ubuntu/project/
```
```bash
  ssh -i flame.pem ubuntu@ec2-44-211-205-144.compute-1.amazonaws.com
```
Then, for each:
```bash
  cd project
```
```bash
  java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar flame.flame.Worker 9001 52.90.29.134:9000
```

## Dependencies and Third-Party Libraries

Flame uses several third-party libraries and resources:

- **Porter Stemmer** - For text stemming in search functionality
  - Source: http://www.tartarus.org/~martin/PorterStemmer
- **JSoup** - For HTML parsing in the web crawler
  - Source: https://jsoup.org/
- **AWS SDK** - For S3 integration and cloud storage
- **English Words Dictionary** - For text indexing
  - Source: https://github.com/dwyl/english-words/blob/master/read_english_dictionary.py
- **Google Search API** - For search completion features
  - Source: https://developers.google.com/cloud-search/docs/reference/rest/v1/query/suggest

## License

This project is provided as-is for educational and research purposes.
