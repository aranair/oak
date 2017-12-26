package config

import (
	"fmt"
	"github.com/spf13/viper"
	"log"
)

type Config struct {
	Db DB
}

type DB struct {
	Name     string
	Username string
	Password string
}

func LoadConfiguration(file string) Config {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(file)

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file, %s", err)
	}

	fmt.Printf("Using config: %s\n", viper.ConfigFileUsed())

	var C Config
	err := viper.Unmarshal(&C)
	if err != nil {
		panic(err)
	}

	// viper.WatchConfig()

	// viper.OnConfigChange(func(e fsnotify.Event) {
	//   fmt.Println("Config file changed:", e.Name)
	// })

	return C
}
