package main

import (
	"context"
	"sync"

	"connectrpc.com/connect"

	client "github.com/aeryx-ai/sock-client"
	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"
)

// rekeyedPuppets is the machine key's PuppetAPI for a server that outlives
// the key (integrations-vzq): `sock machine-key use --force` replaces it in
// the config file while the server runs. A call the hub rejects reads the
// file again, once; another machine key there for the same server replaces
// the key, and the call is tried once more with it. Puppet tokens minted
// before are kept.
type rekeyedPuppets struct {
	reload func() (client.Config, error)
	build  func(client.Config) client.PuppetAPI
	log    func(format string, args ...any)

	mu  sync.Mutex
	key client.Config
	api client.PuppetAPI
}

func newRekeyedPuppets(key client.Config, reload func() (client.Config, error), build func(client.Config) client.PuppetAPI, log func(format string, args ...any)) *rekeyedPuppets {
	return &rekeyedPuppets{reload: reload, build: build, log: log, key: key, api: build(key)}
}

// rejected is the hub's answer to a key it no longer accepts.
func rejected(err error) bool {
	return connect.CodeOf(err) == connect.CodeUnauthenticated || client.Reason(err) == sockv1.ErrorReason_ERROR_REASON_TOKEN_INVALID
}

// rekeyed runs call with the current key, and once more with a replaced one
// when the hub rejected it.
func rekeyed[T any](r *rekeyedPuppets, call func(client.PuppetAPI) (T, error)) (T, error) {
	r.mu.Lock()
	used, api := r.key, r.api
	r.mu.Unlock()
	out, err := call(api)
	if !rejected(err) {
		return out, err
	}
	api, ok := r.rekey(used)
	if !ok {
		return out, err
	}
	return call(api)
}

// rekey is the API of the key that replaced used, false when the file holds
// none: the same key, a key for another server, or no readable file.
func (r *rekeyedPuppets) rekey(used client.Config) (client.PuppetAPI, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.key.Token != used.Token {
		return r.api, true // a concurrent call already swapped it
	}
	fresh, err := r.reload()
	if err != nil || client.TokenKind(fresh.Token) != client.KindMachineKey || fresh.URL != used.URL || fresh.Token == used.Token {
		return nil, false
	}
	r.key, r.api = fresh, r.build(fresh)
	if r.log != nil {
		r.log("the hub rejected the machine key; using the new one in the config file")
	}
	return r.api, true
}

func (r *rekeyedPuppets) CreatePuppet(ctx context.Context, name, label string) (string, string, error) {
	type made struct{ id, token string }
	m, err := rekeyed(r, func(a client.PuppetAPI) (made, error) {
		id, tok, err := a.CreatePuppet(ctx, name, label)
		return made{id, tok}, err
	})
	return m.id, m.token, err
}

func (r *rekeyedPuppets) ListPuppets(ctx context.Context) ([]client.PuppetInfo, error) {
	return rekeyed(r, func(a client.PuppetAPI) ([]client.PuppetInfo, error) { return a.ListPuppets(ctx) })
}

func (r *rekeyedPuppets) MintPuppetToken(ctx context.Context, id, label string) (string, error) {
	return rekeyed(r, func(a client.PuppetAPI) (string, error) { return a.MintPuppetToken(ctx, id, label) })
}

func (r *rekeyedPuppets) ErasePuppet(ctx context.Context, id string) error {
	return r.void(func(a client.PuppetAPI) error { return a.ErasePuppet(ctx, id) })
}

func (r *rekeyedPuppets) ArchivePuppet(ctx context.Context, id string) error {
	return r.void(func(a client.PuppetAPI) error { return a.ArchivePuppet(ctx, id) })
}

func (r *rekeyedPuppets) UnarchivePuppet(ctx context.Context, id string) error {
	return r.void(func(a client.PuppetAPI) error { return a.UnarchivePuppet(ctx, id) })
}

// void is rekeyed for a call that returns only an error.
func (r *rekeyedPuppets) void(call func(client.PuppetAPI) error) error {
	_, err := rekeyed(r, func(a client.PuppetAPI) (struct{}, error) { return struct{}{}, call(a) })
	return err
}
