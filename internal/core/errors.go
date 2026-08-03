package core

import "errors"

var (
	ErrNotFound       = errors.New("not found")
	ErrUnauthorized   = errors.New("unauthorized")
	ErrForbidden      = errors.New("forbidden")
	ErrBadRequest     = errors.New("bad request")
	ErrInternal       = errors.New("internal server error")
	ErrNoSource       = errors.New("no streaming source found")
	ErrRateLimited    = errors.New("rate limited")
	ErrProviderDown   = errors.New("provider unavailable")
)
