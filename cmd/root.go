/*
Copyright © 2020 Blaster Contributors

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
	"fmt"
	"os"

	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var cfgFile string
var handlerCommand string
var handlerArgv []string
var handlerURL string
var maxHandlers int
var enableVerboseLog bool
var startupDelaySeconds int
var retryDelaySeconds uint
var retryCount int

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "blaster",
	Short: "A reliable message processing pipeline",
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if cmd.Name() != "version" && handlerCommand == "" {
			return errors.New("required flag handler-command not set")
		}
		return nil
	},
	SilenceUsage: true,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.PersistentFlags().StringVar(&handlerCommand, "handler-command", "", "name of handler command")
	rootCmd.PersistentFlags().StringSliceVar(&handlerArgv, "handler-args", nil, "arguments to handler")
	rootCmd.PersistentFlags().IntVar(&maxHandlers, "max-handlers", 0, "max number of concurrent handlers")
	rootCmd.PersistentFlags().IntVar(&startupDelaySeconds, "startup-delay-seconds", 5, "number of seconds to wait on start")
	rootCmd.PersistentFlags().BoolVarP(&enableVerboseLog, "verbose", "v", false, "enable verbose logging")
	rootCmd.PersistentFlags().StringVar(&handlerURL, "handler-url", "http://localhost:8312/", "handler endpoint url")
	rootCmd.PersistentFlags().IntVarP(&retryCount, "retry-count", "c", 0, "number of retry attempts")
	rootCmd.PersistentFlags().UintVarP(&retryDelaySeconds, "retry-delay-seconds", "d", 1, "delay between retry attempts")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".blaster" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".blaster")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
