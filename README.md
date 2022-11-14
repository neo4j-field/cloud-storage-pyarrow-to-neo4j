# Data ingestion for Neo4j GDS (via Apache Arrow) 🏹
This project contains python notebooks that load data into a Neo4j Database or Graph Projection
from large datasets consisted of **Apache Parquet** files.

## Managing large datasets with Apache Arrow

One of the main capibilities of Apache Arrow is the ability to deal with <strong>memory-mapped files</strong>,<br>
this allows Arrow to read datasets that are bigger than the available RAM without incurring any additional memory cost.<br>

With the PyArrow ParquetDataset module we can create a dataset of memory-map parquet files, it can then be fragmented and processed in batches.
    
In adition to `local` & `hdfs` storage, PyArrow also supports cloud object storage such as:

* Amazon Simple Storage (S3)
* Google Cloud Storage (GCS)
* Microsoft Azure Blob Storage

![image info](./images/pyarrow_datasets_architecture.png)
## Neo4j GDS Arrow Installation

Please see the [Apache Arrow](https://neo4j.com/docs/graph-data-science/current/installation/installation-apache-arrow/) installation instructions in the Neo4j GDS documentation.

