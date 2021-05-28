[![Go Report Card](https://goreportcard.com/badge/github.com/aws-samples/data-transfer-hub-cli)](https://goreportcard.com/report/github.com/aws-samples/data-transfer-hub-cli)


# data-transfer-hub-cli

dthcli (short for data-transfer-hub-cli) is a distributed tool to transfer data to Amazon S3 from other cloud storage services (including Aliyun OSS, Tencent COS, Qiniu Kodo, etc.) or between AWS regions (cross partition).

## Introduction

This tool leverages Amazon SQS to distribute the transfer processes in many worker nodes. You can run as many worker nodes as required concurrently for large volume of objects. Each worker node will consume the messages from Amazon SQS and start transferring from the source to destination. Each message contains information that represents a object in cloud storage service to be transfered.


## Installation

Download the tool from [Release](https://github.com/aws-samples/data-transfer-hub-cli/releases) page.

For example, on Linux:
```
release=1.0.2
curl -LO "https://github.com/aws-samples/data-transfer-hub-cli/releases/download/v${release}/dthcli_${release}_linux_386.tar.gz"
tar zxvf dthcli_${release}_linux_386.tar.gz
```

To verify, simply run `./dthcli version`
```
$ ./dthcli version
drhcli version vX.Y.Z
```

> Or you can clone this repo and build (go build) by yourself (Go Version >= v1.16)


## Prerequisites

You can run this tool in any places, even in local. However, you will need to have a SQS Queue and a DynamoDB table created before using the tool. DynamoDB is used to store the replication status of each objects, the partition key for DynamoDB must be `ObjectKey`

If you need to provide AK/SK to accessing cloud service, you will need to set up a credential secure string in Amazon Secrets Manager with a format as below

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
srcBucket: src-bucket
srcRegion: us-west-2

destBucket: dest-bucket
destRegion: cn-north-1

jobTableName: test-table
jobQueueName: test-queue
```

By default, this tool will try to read a `config.yaml` in the same folder, if you create the configuration file in a different folder or with a different file name, please use extra option `--config xxx.yaml` to load your config file.


## Usage

Run `dthcli help` for more details.
```
$ ./dthcli help
A distributed CLI to replicate data to Amazon S3 from other cloud storage services.

Usage:
  dthcli [command]

Available Commands:
  help        Help about any command
  run         Start running a job
  version     Print the version number

Flags:
      --config string   config file (default is ./config.yaml)
  -h, --help            help for dthcli

Use "dthcli [command] --help" for more information about a command.
```

To actually start the job, use `dthcli run` command.

- Start Finder Job

```
./dthcli run -t Finder
```

- Start Worker Job

```
./dthcli run -t Worker
```
