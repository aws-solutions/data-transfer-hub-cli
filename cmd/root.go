/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.a2z.com/dthcli/dth"
)

// Version Number
const Version = "v1.0.0"

var (
	cfgFile, jobType string
	cfg              *dth.JobConfig
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "dthcli",
	Short: "A distributed CLI to transfer data to Amazon S3",
	Long:  `A distributed CLI to transfer data to Amazon S3 from other cloud storage services.`,

	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")

	runCmd.Flags().StringVarP(&jobType, "type", "t", "Finder", "Job Type, choose either Finder or Worker")

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(runCmd)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetDefault("srcType", "Amazon_S3")
	viper.SetDefault("destStorageClass", "STANDARD")
	viper.SetDefault("srcPrefix", "")
	viper.SetDefault("srcPrefixList", "")
	viper.SetDefault("srcCredential", "")
	viper.SetDefault("srcEndpoint", "")
	viper.SetDefault("destPrefix", "")
	viper.SetDefault("destCredential", "")
	viper.SetDefault("destAcl", "bucket-owner-full-control")

	viper.SetDefault("options.chunkSize", dth.DefaultChunkSize)
	viper.SetDefault("options.multipartThreshold", dth.DefaultMultipartThreshold)
	viper.SetDefault("options.maxKeys", dth.DefaultMaxKeys)
	viper.SetDefault("options.messageBatchSize", dth.DefaultMessageBatchSize)
	viper.SetDefault("options.finderDepth", dth.DefaultFinderDepth)
	viper.SetDefault("options.finderNumber", dth.DefaultFinderNumber)
	viper.SetDefault("options.workerNumber", dth.DefaultWorkerNumber)
	viper.SetDefault("options.includeMetadata", false)

	viper.BindEnv("srcType", "SOURCE_TYPE")
	viper.BindEnv("srcBucket", "SRC_BUCKET")
	viper.BindEnv("srcPrefix", "SRC_PREFIX")
	viper.BindEnv("srcPrefixList", "SRC_PREFIX_LIST")
	viper.BindEnv("srcRegion", "SRC_REGION")
	viper.BindEnv("srcEndpoint", "SRC_ENDPOINT")
	viper.BindEnv("srcCredential", "SRC_CREDENTIALS")
	viper.BindEnv("srcInCurrentAccount", "SRC_IN_CURRENT_ACCOUNT")
	viper.BindEnv("skipCompare", "SKIP_COMPARE")

	viper.BindEnv("destBucket", "DEST_BUCKET")
	viper.BindEnv("destPrefix", "DEST_PREFIX")
	viper.BindEnv("destRegion", "DEST_REGION")
	viper.BindEnv("destCredential", "DEST_CREDENTIALS")
	viper.BindEnv("destInCurrentAccount", "DEST_IN_CURRENT_ACCOUNT")
	viper.BindEnv("destStorageClass", "DEST_STORAGE_CLASS")
	viper.BindEnv("destAcl", "DEST_ACL")

	viper.BindEnv("jobTableName", "JOB_TABLE_NAME")
	viper.BindEnv("jobQueueName", "JOB_QUEUE_NAME")

	viper.BindEnv("options.maxKeys", "MAX_KEYS")
	viper.BindEnv("options.chunkSize", "CHUNK_SIZE")
	viper.BindEnv("options.multipartThreshold", "MULTIPART_THRESHOLD")
	viper.BindEnv("options.messageBatchSize", "MESSAGE_BATCH_SIZE")
	viper.BindEnv("options.finderDepth", "FINDER_DEPTH")
	viper.BindEnv("options.finderNumber", "FINDER_NUMBER")
	viper.BindEnv("options.workerNumber", "WORKER_NUMBER")
	viper.BindEnv("options.includeMetadata", "INCLUDE_METADATA")

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Default config file is "./config.yaml"
		viper.AddConfigPath(".")
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	viper.ReadInConfig()
	if err := viper.ReadInConfig(); err == nil {
		log.Println("Using config file:", viper.ConfigFileUsed())
	}

	options := &dth.JobOptions{
		ChunkSize:          viper.GetInt("options.chunkSize"),
		MultipartThreshold: viper.GetInt("options.multipartThreshold"),
		MaxKeys:            viper.GetInt32("options.maxKeys"),
		MessageBatchSize:   viper.GetInt("options.messageBatchSize"),
		FinderDepth:        viper.GetInt("options.finderDepth"),
		FinderNumber:       viper.GetInt("options.finderNumber"),
		WorkerNumber:       viper.GetInt("options.workerNumber"),
		IncludeMetadata:    viper.GetBool("options.includeMetadata"),
	}

	cfg = &dth.JobConfig{
		SrcType:              viper.GetString("srcType"),
		SrcBucket:            viper.GetString("srcBucket"),
		SrcPrefix:            viper.GetString("srcPrefix"),
		SrcPrefixList:        viper.GetString("srcPrefixList"),
		SrcRegion:            viper.GetString("srcRegion"),
		SrcEndpoint:          viper.GetString("srcEndpoint"),
		SrcCredential:        viper.GetString("srcCredential"),
		SrcInCurrentAccount:  viper.GetBool("srcInCurrentAccount"),
		SkipCompare:          viper.GetBool("skipCompare"),
		DestBucket:           viper.GetString("destBucket"),
		DestPrefix:           viper.GetString("destPrefix"),
		DestRegion:           viper.GetString("destRegion"),
		DestCredential:       viper.GetString("destCredential"),
		DestStorageClass:     viper.GetString("destStorageClass"),
		DestAcl:              viper.GetString("destAcl"),
		DestInCurrentAccount: viper.GetBool("destInCurrentAccount"),
		JobTableName:         viper.GetString("jobTableName"),
		JobQueueName:         viper.GetString("jobQueueName"),
		JobOptions:           options,
	}

}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Long:  "Print the version number",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("dthcli version %s\n", Version)
	},
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Start running a job",
	Long: `Start running a job.

For example: dthcli run -t Finder

Supported types:
	- Finder: Finder is a job that lists and compares objects in the source and target buckets, and sends the delta list to SQS Queue.
	- Worker: Worker is a job that consumes the messages from SQS Queue and start the migration
`,
	Run: func(cmd *cobra.Command, args []string) {

		if cfg.SrcBucket == "" || cfg.DestBucket == "" {
			log.Fatalf("Cannot find source or destination bucket name, please check if you have run with a config file or environment variables. Run `dthcli help` for more details")
		}

		log.Printf("Start running %s job", jobType)
		ctx := context.Background()

		var job dth.Job

		switch jobType {
		case "Finder":
			job = dth.NewFinder(ctx, cfg)

		case "Worker":
			job = dth.NewWorker(ctx, cfg)

		default:
			log.Fatalf("Unknown Job Type - %s. Type must be either Finder or Worker\n, please start again", jobType)

		}
		job.Run(ctx)
	},
}
