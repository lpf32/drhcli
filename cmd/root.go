/*
Copyright © 2021 Aiden Dai daixb@amazon.com

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

	"github.com/daixba/drhcli/drh"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Version Number
const Version = "v0.1.0"

var (
	cfgFile, jobType string
	cfg              *drh.JobConfig
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "drhcli",
	Short: "A cli to run data replication hub jobs",
	Long: `A cli to run data replication hub jobs,

Find more information at: https://github.com/daixba/drhcli`,

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

	runCmd.Flags().StringVarP(&jobType, "type", "t", "Finder", "Job Type, choose either Finder or Worker (default type is Finder)")

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(runCmd)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.SetDefault("srcType", "Amazon_S3")
	viper.SetDefault("desStorageClass", "STANDARD")
	viper.SetDefault("srcBucketPrefix", "")
	viper.SetDefault("destBucketPrefix", "")

	viper.SetDefault("options.chunkSize", drh.ChunkSize)
	viper.SetDefault("options.multipartThreshold", drh.MultipartThreshold)
	viper.SetDefault("options.maxKeys", drh.MaxKeys)

	viper.BindEnv("srcType", "SOURCE_TYPE")
	viper.BindEnv("srcBucketName", "SRC_BUCKET_NAME")
	viper.BindEnv("srcBucketPrefix", "SRC_BUCKET_PREFIX")
	viper.BindEnv("srcRegion", "SRC_REGION")
	viper.BindEnv("srcCredential", "SRC_CREDENTIALS")

	viper.BindEnv("destBucketName", "DEST_BUCKET_NAME")
	viper.BindEnv("destBucketPrefix", "DEST_BUCKET_PREFIX")
	viper.BindEnv("destRegion", "DEST_REGION")
	viper.BindEnv("destCredential", "DEST_CREDENTIALS")
	viper.BindEnv("destStorageClass", "DEST_STORAGE_CLASS")

	viper.BindEnv("jobTableName", "JOB_TABLE_NAME")
	viper.BindEnv("jobQueueName", "JOB_QUEUE_NAME")

	viper.BindEnv("options.maxKeys", "MAX_KEYS")
	viper.BindEnv("options.chunkSize", "CHUNK_SIZE")
	viper.BindEnv("options.multipartThreshold", "MULTIPART_THRESHOLD")

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
	// if err := viper.ReadInConfig(); err == nil {
	// 	fmt.Println("Using config file:", viper.ConfigFileUsed())
	// }

	cfg = &drh.JobConfig{
		SrcType:            viper.GetString("srcType"),
		SrcBucketName:      viper.GetString("srcBucketName"),
		SrcBucketPrefix:    viper.GetString("srcBucketPrefix"),
		SrcRegion:          viper.GetString("srcRegion"),
		SrcCredential:      viper.GetString("srcCredential"),
		DestBucketName:     viper.GetString("destBucketName"),
		DestBucketPrefix:   viper.GetString("destBucketPrefix"),
		DestRegion:         viper.GetString("destRegion"),
		DestCredential:     viper.GetString("destCredential"),
		DestStorageClass:   viper.GetString("destStorageClass"),
		JobTableName:       viper.GetString("jobTableName"),
		JobQueueName:       viper.GetString("jobQueueName"),
		ChunkSize:          viper.GetInt("options.chunkSize"),
		MultipartThreshold: viper.GetInt("options.multipartThreshold"),
		MaxKeys:            viper.GetInt("options.maxKeys"),
	}

}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Long:  "Print the version number",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("drhcli version %s\n", Version)
	},
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Start running a job",
	Long: `Start running a job.

For example: drhcli run -t Finder

Supported types:
	- Finder: Finder is a job that lists and compares objects in source and target buckets, and gets the delta and sends the list to SQS Queue.
	- Worker: Worker is a job that consumes the messages from SQS Queue and start the migration
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf("Start running %s job", jobType)

		switch jobType {
		case "Finder":
			ctx := context.TODO()
			f := drh.InitFinder(ctx, cfg)
			f.CompareAndSend()

		case "Worker":
			log.Println("To be updated")

		default:
			log.Fatalf("Unknown Job Type - %s. Type must be either Finder or Worker\n, please start again", jobType)

		}
	},
}
