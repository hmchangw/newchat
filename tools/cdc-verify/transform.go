package main

import (
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

type transformFn func(args []any) (any, error)

type transformRegistry map[string]transformFn

// transformNames mirrors the registry keys so validation can check names
// without a bucket Sizer. Keep in sync with newTransformRegistry.
var transformNames = map[string]bool{
	"unixMilli": true,
	"toString":  true,
	"msgBucket": true,
}

func knownTransform(name string) bool { return transformNames[name] }

func newTransformRegistry(sizer msgbucket.Sizer) transformRegistry {
	return transformRegistry{
		"unixMilli": func(args []any) (any, error) {
			return coerceUnixMilli(args)
		},
		"toString": func(args []any) (any, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("toString takes 1 arg, got %d", len(args))
			}
			switch args[0].(type) {
			case map[string]any, []any:
				return nil, fmt.Errorf("toString: composite value %T not supported", args[0])
			}
			return fmt.Sprintf("%v", args[0]), nil
		},
		"msgBucket": func(args []any) (any, error) {
			ms, err := coerceUnixMilli(args)
			if err != nil {
				return nil, err
			}
			return sizer.Of(time.UnixMilli(ms)), nil
		},
	}
}

func coerceUnixMilli(args []any) (int64, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("expected 1 arg, got %d", len(args))
	}
	switch v := args[0].(type) {
	case time.Time:
		return v.UTC().UnixMilli(), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("cannot coerce %T to unix millis", args[0])
	}
}

// apply runs the named transform; the empty name is the identity on args[0].
func (r transformRegistry) apply(name string, args []any) (any, error) {
	if name == "" {
		if len(args) != 1 {
			return nil, fmt.Errorf("identity transform takes 1 arg, got %d", len(args))
		}
		return args[0], nil
	}
	fn, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("unknown transform %q", name)
	}
	return fn(args)
}
