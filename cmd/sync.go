/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	taossync "github.com/ztrade/taossync/sync"
)

var (
	strStart string
	strEnd   string
)

// syncCmd represents the sync command
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "sync taosdb stable between two server",
	Long:  `sync taosdb stable between two server, if start or end is empty, just sync last one hour data`,
	Run:   runSync,
}

func init() {
	rootCmd.AddCommand(syncCmd)
	syncCmd.PersistentFlags().StringVar(&strStart, "start", "", "start time, time.RFC3339, 2022-09-30T00:00:00+08:00")
	syncCmd.PersistentFlags().StringVar(&strEnd, "end", "", "end time, time.RFC3339, 2022-09-30T00:00:00+08:00")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// syncCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// syncCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func runSync(cmd *cobra.Command, args []string) {
	var cfg taossync.SyncConfig
	err := viper.Unmarshal(&cfg)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	ts, err := taossync.NewTaosSync(&cfg)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	var tStart, tEnd time.Time
	if strStart == "" || strEnd == "" {
		t := time.Now()
		tEnd = time.Unix(0, (t.UnixNano()/int64(time.Hour))*int64(time.Hour))
		tStart = tEnd.Add(-time.Hour)
	} else {
		tStart, err = time.Parse(time.RFC3339, strStart)
		if err != nil {
			fmt.Println("parse start failed:", err.Error())
			return
		}
		tEnd, err = time.Parse(time.RFC3339, strEnd)
		if err != nil {
			fmt.Println("parse end failed", err.Error())
			return
		}
	}
	err = ts.Sync(tStart, tEnd)
	if err != nil {
		fmt.Println("sync error:", err.Error())
	}
}
