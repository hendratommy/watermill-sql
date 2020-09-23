package sql

import (
	"context"

	"github.com/jackc/pgtype/pgxtype"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
)

func initializeSchema(
	ctx context.Context,
	topic string,
	logger watermill.LoggerAdapter,
	db contextExecutor,
	schemaAdapter SchemaAdapter,
	offsetsAdapter OffsetsAdapter,
) error {
	err := validateTopicName(topic)
	if err != nil {
		return err
	}

	initializingQueries := schemaAdapter.SchemaInitializingQueries(topic)
	if offsetsAdapter != nil {
		initializingQueries = append(initializingQueries, offsetsAdapter.SchemaInitializingQueries(topic)...)
	}

	logger.Info("Initializing subscriber schema", watermill.LogFields{
		"query": initializingQueries,
	})

	for _, q := range initializingQueries {
		_, err := db.ExecContext(ctx, q)
		if err != nil {
			return errors.Wrap(err, "cound not initialize schema")
		}
	}

	return nil
}

func initializePgxSchema(
	ctx context.Context,
	topic string,
	logger watermill.LoggerAdapter,
	db pgxtype.Querier,
	schemaAdapter SchemaAdapter,
	offsetsAdapter OffsetsAdapter,
) error {
	err := validateTopicName(topic)
	if err != nil {
		return err
	}

	initializingQueries := schemaAdapter.SchemaInitializingQueries(topic)
	if offsetsAdapter != nil {
		initializingQueries = append(initializingQueries, offsetsAdapter.SchemaInitializingQueries(topic)...)
	}

	logger.Info("Initializing subscriber schema", watermill.LogFields{
		"query": initializingQueries,
	})

	for _, q := range initializingQueries {
		_, err := db.Exec(ctx, q)
		if err != nil {
			return errors.Wrap(err, "cound not initialize schema")
		}
	}

	return nil
}
