package sql

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Subscriber makes SELECT queries on the chosen table with the interval defined in the config.
// The rows are unmarshaled into Watermill messages.
type PgxSubscriber struct {
	consumerIdBytes  []byte
	consumerIdString string

	db     pgxBeginner
	config SubscriberConfig

	subscribeWg *sync.WaitGroup
	closing     chan struct{}
	closed      bool

	logger watermill.LoggerAdapter
}

func NewPgxSubscriber(db pgxBeginner, config SubscriberConfig, logger watermill.LoggerAdapter) (*PgxSubscriber, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	config.setDefaults()
	err := config.validate()
	if err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	idBytes, idStr, err := newSubscriberID()
	if err != nil {
		return &PgxSubscriber{}, errors.Wrap(err, "cannot generate subscriber id")
	}
	logger = logger.With(watermill.LogFields{"subscriber_id": idStr})

	sub := &PgxSubscriber{
		consumerIdBytes:  idBytes,
		consumerIdString: idStr,

		db:     db,
		config: config,

		subscribeWg: &sync.WaitGroup{},
		closing:     make(chan struct{}),

		logger: logger,
	}

	return sub, nil
}

func (s *PgxSubscriber) Subscribe(ctx context.Context, topic string) (o <-chan *message.Message, err error) {
	if s.closed {
		return nil, ErrSubscriberClosed
	}

	if err = validateTopicName(topic); err != nil {
		return nil, err
	}

	if s.config.InitializeSchema {
		if err := s.SubscribeInitialize(topic); err != nil {
			return nil, err
		}
	}

	// the information about closing the subscriber is propagated through ctx
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan *message.Message)

	s.subscribeWg.Add(1)
	go func() {
		s.consume(ctx, topic, out)
		close(out)
		cancel()
	}()

	return out, nil
}

func (s *PgxSubscriber) consume(ctx context.Context, topic string, out chan *message.Message) {
	defer s.subscribeWg.Done()

	logger := s.logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": s.config.ConsumerGroup,
	})

	var sleepTime time.Duration = 0
	for {
		select {
		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return

		case <-ctx.Done():
			logger.Info("Stopping consume, context canceled", nil)
			return

		case <-time.After(sleepTime): // Wait if needed
			sleepTime = 0

		}

		messageUUID, noMsg, err := s.query(ctx, topic, out, logger)
		if err != nil {
			if isDeadlock(err) {
				logger.Debug("Deadlock during querying message, trying again", watermill.LogFields{
					"err":          err.Error(),
					"message_uuid": messageUUID,
				})
			} else {
				logger.Error("Error querying for message", err, watermill.LogFields{
					"wait_time": s.config.RetryInterval,
				})
				sleepTime = s.config.RetryInterval
			}
		} else if noMsg {
			// wait until polling for the next message
			logger.Debug("No messages, waiting until next query", watermill.LogFields{
				"wait_time": s.config.PollInterval,
			})
			sleepTime = s.config.PollInterval
		}
	}
}

func (s *PgxSubscriber) query(
	ctx context.Context,
	topic string,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) (messageUUID string, noMsg bool, err error) {
	txOptions := pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	}
	tx, err := s.db.BeginTx(ctx, txOptions)
	if err != nil {
		return "", false, errors.Wrap(err, "could not begin tx for querying")
	}

	defer func() {
		if err != nil {
			rollbackErr := tx.Rollback(context.TODO())
			if rollbackErr != nil && rollbackErr != pgx.ErrTxClosed {
				logger.Error("could not rollback tx for querying message", rollbackErr, nil)
			}
		} else {
			commitErr := tx.Commit(context.TODO())
			if commitErr != nil && commitErr != pgx.ErrTxClosed {
				logger.Error("could not commit tx for querying message", commitErr, nil)
			}
		}
	}()

	selectQuery, selectQueryArgs := s.config.SchemaAdapter.SelectQuery(
		topic,
		s.config.ConsumerGroup,
		s.config.OffsetsAdapter,
	)
	logger.Trace("Querying message", watermill.LogFields{
		"query":      selectQuery,
		"query_args": sqlArgsToLog(selectQueryArgs),
	})
	row := tx.QueryRow(ctx, selectQuery, selectQueryArgs...)

	offset, msg, err := s.config.SchemaAdapter.UnmarshalMessage(row)
	if errors.Cause(err) == pgx.ErrNoRows {
		return "", true, nil
	} else if err != nil {
		return "", false, errors.Wrap(err, "could not unmarshal message from query")
	}

	logger = logger.With(watermill.LogFields{
		"msg_uuid": msg.UUID,
	})
	logger.Trace("Received message", nil)

	consumedQuery, consumedArgs := s.config.OffsetsAdapter.ConsumedMessageQuery(
		topic,
		offset,
		s.config.ConsumerGroup,
		s.consumerIdBytes,
	)
	if consumedQuery != "" {
		logger.Trace("Executing query to confirm message consumed", watermill.LogFields{
			"query":      consumedQuery,
			"query_args": sqlArgsToLog(consumedArgs),
		})

		_, err := tx.Exec(ctx, consumedQuery, consumedArgs...)
		if err != nil {
			return msg.UUID, false, errors.Wrap(err, "cannot send consumed query")
		}
	}

	acked := s.sendMessage(ctx, msg, out, logger)
	if acked {
		ackQuery, ackArgs := s.config.OffsetsAdapter.AckMessageQuery(topic, offset, s.config.ConsumerGroup)

		logger.Trace("Executing ack message query", watermill.LogFields{
			"query":      ackQuery,
			"query_args": sqlArgsToLog(ackArgs),
		})

		result, err := tx.Exec(ctx, ackQuery, ackArgs...)
		if err != nil {
			return msg.UUID, false, errors.Wrap(err, "could not get args for acking the message")
		}

		rowsAffected := result.RowsAffected()

		logger.Trace("Executed ack message query", watermill.LogFields{
			"rows_affected": rowsAffected,
		})
	}

	return msg.UUID, false, nil
}

// sendMessages sends messages on the output channel.
func (s *PgxSubscriber) sendMessage(
	ctx context.Context,
	msg *message.Message,
	out chan *message.Message,
	logger watermill.LoggerAdapter,
) (acked bool) {
	msgCtx, cancel := context.WithCancel(ctx)
	msg.SetContext(msgCtx)
	defer cancel()

ResendLoop:
	for {

		select {
		case out <- msg:

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}

		select {
		case <-msg.Acked():
			logger.Debug("Message acked by subscriber", nil)
			return true

		case <-msg.Nacked():
			//message nacked, try resending
			logger.Debug("Message nacked, resending", nil)
			msg = msg.Copy()

			if s.config.ResendInterval != 0 {
				time.Sleep(s.config.ResendInterval)
			}

			continue ResendLoop

		case <-s.closing:
			logger.Info("Discarding queued message, subscriber closing", nil)
			return false

		case <-ctx.Done():
			logger.Info("Discarding queued message, context canceled", nil)
			return false
		}
	}
}

func (s *PgxSubscriber) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true

	close(s.closing)
	s.subscribeWg.Wait()

	return nil
}

func (s *PgxSubscriber) SubscribeInitialize(topic string) error {
	return initializePgxSchema(
		context.Background(),
		topic,
		s.logger,
		s.db,
		s.config.SchemaAdapter,
		s.config.OffsetsAdapter,
	)
}
