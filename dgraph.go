// Package dgraph provides a GoHub database implementation backed by Dgraph.
package dgraph

import (
	"context"
	"errors"
	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/graphql-go/graphql"
	"github.com/mughub/mughub/db"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"io"
)

// DB is a database implementation backed by Dgraph
type DB struct {
	c      *dgo.Dgraph
	schema graphql.Schema
}

// Init initializes a Dgraph database.
func (d *DB) Init(schema io.Reader, cfg *viper.Viper) (err error) {
	// Get Dgraph addr
	addr := cfg.GetString("addr")
	if addr == "" {
		return errors.New("db: dgraph addr must be provided")
	}

	// Connect to Dgraph with gRPC
	d.c, err = d.connect(addr)
	if err != nil {
		return
	}

	// TODO: Add GraphQL Resolvers

	// Setup up Dgraph Schema if not already configured
	return d.setup()
}

func (d *DB) connect(addr string) (*dgo.Dgraph, error) {
	dc, err := grpc.Dial(addr, grpc.WithInsecure())
	return dgo.NewDgraphClient(api.NewDgraphClient(dc)), err
}

func (d *DB) setup() error {
	return nil
}

// Do executes a GraphQL request against the Dgraph database.
func (d *DB) Do(ctx context.Context, req string, vars map[string]interface{}) *db.Result {
	res := graphql.Do(graphql.Params{
		Context:        ctx,
		RequestString:  req,
		VariableValues: vars,
	})
	return &db.Result{
		Data: res.Data,
		// TODO: Transfer errors
	}
}
