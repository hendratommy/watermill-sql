package sql

import (
	"context"
	"sync"

	"github.com/jackc/pgtype/pgxtype"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// PgxPublisher inserts the Messages as rows into a SQL table..
type PgxPublisher struct {
	config PublisherConfig

	db pgxtype.Querier

	publishWg *sync.WaitGroup
	closeCh   chan struct{}
	closed    bool

	initializedTopics sync.Map
	logger            watermill.LoggerAdapter
}

func NewPgxPublisher(db pgxtype.Querier, config PublisherConfig, logger watermill.LoggerAdapter) (*PgxPublisher, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if db == nil {
		return nil, errors.New("db is nil")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	if config.AutoInitializeSchema && isPgxTx(db) {
		// either use a prior schema with a tx db handle, or don't use tx with AutoInitializeSchema
		return nil, errors.New("tried to use AutoInitializeSchema with a database handle that looks like" +
			"an ongoing transaction; this may result in an implicit commit")
	}

	return &PgxPublisher{
		config: config,
		db:     db,

		publishWg: new(sync.WaitGroup),
		closeCh:   make(chan struct{}),
		closed:    false,

		logger: logger,
	}, nil
}

// Publish inserts the messages as rows into the MessagesTable.
// Order is guaranteed for messages within one call.
// Publish is blocking until all rows have been added to the Publisher's transaction.
// PgxPublisher doesn't guarantee publishing messages in a single transaction,
// but the constructor accepts both anything that satisfy pgxtype.Querier, so transactions may be handled upstream by the user.
func (p *PgxPublisher) Publish(topic string, messages ...*message.Message) (err error) {
	if p.closed {
		return ErrPublisherClosed
	}

	p.publishWg.Add(1)
	defer p.publishWg.Done()

	if err := validateTopicName(topic); err != nil {
		return err
	}

	if err := p.initializeSchema(topic); err != nil {
		return err
	}

	insertQuery, insertArgs, err := p.config.SchemaAdapter.InsertQuery(topic, messages)
	if err != nil {
		return errors.Wrap(err, "cannot create insert query")
	}

	p.logger.Trace("Inserting message to SQL", watermill.LogFields{
		"query":      insertQuery,
		"query_args": sqlArgsToLog(insertArgs),
	})

	_, err = p.db.Exec(context.Background(), insertQuery, insertArgs...)
	if err != nil {
		return errors.Wrap(err, "could not insert message as row")
	}

	return nil
}

func (p *PgxPublisher) initializeSchema(topic string) error {
	if !p.config.AutoInitializeSchema {
		return nil
	}

	if _, ok := p.initializedTopics.Load(topic); ok {
		return nil
	}

	if err := initializePgxSchema(
		context.Background(),
		topic,
		p.logger,
		p.db,
		p.config.SchemaAdapter,
		nil,
	); err != nil {
		return errors.Wrap(err, "cannot initialize schema")
	}

	p.initializedTopics.Store(topic, struct{}{})
	return nil
}

// Close closes the publisher, which means that all the Publish calls called before are finished
// and no more Publish calls are accepted.
// Close is blocking until all the ongoing Publish calls have returned.
func (p *PgxPublisher) Close() error {
	if p.closed {
		return nil
	}

	p.closed = true

	close(p.closeCh)
	p.publishWg.Wait()

	return nil
}

func isPgxTx(db pgxtype.Querier) bool {
	_, dbIsTx := db.(interface {
		Commit(context.Context) error
		Rollback(context.Context) error
	})
	return dbIsTx
}
