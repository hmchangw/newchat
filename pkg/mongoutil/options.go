package mongoutil

import "go.mongodb.org/mongo-driver/v2/mongo/options"

// queryOptions: WithSort/Limit/Skip only affect FindMany.
type queryOptions struct {
	projection any
	sort       any
	limit      *int64
	skip       *int64
}

func (qo *queryOptions) findOneOpts() *options.FindOneOptionsBuilder {
	opts := options.FindOne()
	if qo.projection != nil {
		opts.SetProjection(qo.projection)
	}
	return opts
}

func (qo *queryOptions) findOpts() *options.FindOptionsBuilder {
	opts := options.Find()
	if qo.projection != nil {
		opts.SetProjection(qo.projection)
	}
	if qo.sort != nil {
		opts.SetSort(qo.sort)
	}
	if qo.limit != nil {
		opts.SetLimit(*qo.limit)
	}
	if qo.skip != nil {
		opts.SetSkip(*qo.skip)
	}
	return opts
}

type QueryOption func(*queryOptions)

func WithProjection(projection any) QueryOption {
	return func(o *queryOptions) { o.projection = projection }
}

func WithSort(sort any) QueryOption {
	return func(o *queryOptions) { o.sort = sort }
}

func WithLimit(limit int64) QueryOption {
	return func(o *queryOptions) { o.limit = &limit }
}

func WithSkip(skip int64) QueryOption {
	return func(o *queryOptions) { o.skip = &skip }
}

func apply(opts []QueryOption) *queryOptions {
	qo := &queryOptions{}
	for _, opt := range opts {
		opt(qo)
	}
	return qo
}
