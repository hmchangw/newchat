package main

import "github.com/caarlos0/env/v11"

type Config struct {
	Port          string `env:"PORT" envDefault:"8082"`
	SiteID        string `env:"SITE_ID,required"`
	MongoURI      string `env:"MONGO_URI,required"`
	MongoDB       string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername string `env:"MONGO_USERNAME"`
	MongoPassword string `env:"MONGO_PASSWORD"`
	BcryptCost    int    `env:"BCRYPT_COST" envDefault:"10"`
}

func loadConfig() (Config, error) {
	var c Config
	if err := env.Parse(&c); err != nil {
		return Config{}, err
	}
	return c, nil
}
