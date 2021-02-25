# drhcli

A distributed CLI to replicate data to Amazon S3 from other cloud storage services.

## Introduction

This tool leverages Amazon SQS to distribute the replication processes in many worker nodes. You can run as many worker nodes as required concurrently for large volume of objects. Each worker node will consume the messages from Amazon SQS and start transferring from the source to destination. Each message contains information that represents a object in cloud storage service to be replicated.


## Installation

Download the tool from [Release](https://github.com/daixba/drhcli/releases) page.

For example, on Linux:
```
release=0.1.0
curl -LO "https://github.com/daixba/drhcli/releases/download/v${release}/drhcli_${release}_linux_386.tar.gz"
tar zxvf drhcli_${release}_linux_386.tar.gz
```



To verify, simply run `./drhcli version`
```
$ ./drhcli version
drhcli version vX.Y.Z
```

> Or you can clone this repo and build by yourself (Go Version >= v1.16)


## Prerequisites

You can run this tool in any places, even in local. However, you will need to have a SQS Queue and a DynamoDB table created before using the tool. DyanmoDB is used to store the replication status of each objects.

If you need to provide AK/SK to accessing cloud service, you will need to set up a credential secure string in Amazon System Manager Parameter Store with a format as below

```
{
  "access_key_id": "<Your Access Key ID>",
  "secret_access_key": "<Your Access Key Secret>"
}
```

## Configuration

The job information such as Source Bucket, Destination Bucket etc. are provided in either a config file or via environment variables. 

An example of full config file can be found [here](./config-example.yaml) 

You must provide at least the minimum information as below:
```yaml
srcBucketName: src-bucket
srcRegion: us-west-2

destBucketName: dest-bucket
destRegion: cn-north-1

jobTableName: test-table
jobQueueName: test-queue
```

By default, this tool will try to read a `config.yaml` in the same folder, if you create the configuration file in a different folder or with a different file name, please use extra option `--config xxx.yaml` to load your config file.


## Usage

Run `drhcli help` for more details.
```
$ drhcli help
A distributed CLI to replicate data to Amazon S3 from other cloud storage services.

Find more information at: https://github.com/daixba/drhcli

Usage:
  drhcli [command]

Available Commands:
  help        Help about any command
  run         Start running a job
  version     Print the version number

Flags:
      --config string   config file (default is ./config.yaml)
  -h, --help            help for drhcli

Use "drhcli [command] --help" for more information about a command.
```

To actually start the job, use `drhcli run` command.

- Start Finder Job

```
drhcli run -t Finder
```

- Start Worker

```
drhcli run -t Worker
```
