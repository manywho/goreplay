package main

import (
	"io"
	"log"
	"github.com/jackc/pgx"

	"time"
	"strconv"
)

// PostgreSQLOutput is used for sending payloads to PostgreSQL
type PostgreSQLOutput struct {
	config *PostgreSQLConfig
	pool   *pgx.ConnPool
}

// NewPostgreSQLOutput constructor for PostgreSQLOutput
func NewPostgreSQLOutput(address string, config *PostgreSQLConfig) io.Writer {
	c, err := pgx.ParseConnectionString(config.uri)
	if err != nil {
		log.Fatalln("Failed to parse connection string:", err)
	}

	pool, err := pgx.NewConnPool(pgx.ConnPoolConfig{ConnConfig: c})
	if err != nil {
		log.Fatalln("Failed to create connection pool:", err)
	}

	o := &PostgreSQLOutput{
		config: config,
		pool:   pool,
	}

	return o
}

func (o *PostgreSQLOutput) Write(data []byte) (n int, err error) {
	meta := payloadMeta(data)

	conn, err := o.pool.Acquire()
	if err != nil {
		log.Fatalln("Failed to acquire connection from the pool:", err)
	}

	timestamp, err := strconv.ParseInt(string(meta[2]), 10, 64)
	if err != nil {
		log.Fatalln("Could not parse timestamp:", err)
	}

	_, err = conn.Exec("INSERT INTO goreplay (id, type, occurred_at, data) VALUES ($1, $2, $3, $4)", string(meta[1]), string(meta[0]), time.Unix(0, timestamp), data)
	if err != nil {
		log.Fatalln("Failed to insert payload into PostgreSQL:", err)
	}

	return len(data), nil
}