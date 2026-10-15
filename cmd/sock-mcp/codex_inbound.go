package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"

	client "github.com/guygrigsby/sock-client"
	sockv1 "github.com/guygrigsby/sock-contract/gen/go/sock/v1"

	"github.com/guygrigsby/sock-mcp/internal/asks"
	"github.com/guygrigsby/sock-mcp/internal/channel"
	"github.com/guygrigsby/sock-mcp/internal/codex"
	"github.com/guygrigsby/sock-mcp/sockclient"
)

// codexInbound is the Codex plugin server's listener (sock-integrations ADR
// 2). Of every such server on the machine, the one holding the lock listens:
// it subscribes as the puppet of each project that has a loaded Codex session
// and puts the owner's replies into those sessions through the app-server
// daemon. The rest wait on the lock and take over when its session ends; the
// operating system frees a dead holder's lock.
type codexInbound struct {
	key    client.Config // the machine key, whose server the project puppets share
	daemon codex.Daemon
	asks   string
	getenv func(string) string
	log    *slog.Logger

	mu      sync.Mutex
	serving map[string]bool // projects with a subscription
}

func newCodexInbound(key client.Config, getenv func(string) string, stderr io.Writer) (*codexInbound, error) {
	dir, err := asks.Dir(getenv)
	if err != nil {
		return nil, err
	}
	return &codexInbound{
		key:     key,
		daemon:  codex.Daemon{Socket: codex.DefaultSocket(getenv), Version: version},
		asks:    dir,
		getenv:  getenv,
		log:     slog.New(slog.NewTextHandler(stderr, nil)).With("component", "codex-inbound"),
		serving: map[string]bool{},
	}, nil
}

// Run waits for the lock, then listens until ctx ends.
func (l *codexInbound) Run(ctx context.Context) {
	unlock, err := l.lock(ctx)
	if err != nil {
		if ctx.Err() == nil {
			l.log.Error("no Codex inbound: cannot take the listener lock", "err", err)
		}
		return
	}
	defer unlock()
	l.log.Info("listening for the owner's replies to Codex sessions")
	rescan := make(chan struct{}, 1)
	go l.watch(ctx, rescan)
	for {
		select {
		case <-ctx.Done():
			return
		case <-rescan:
			l.rescan(ctx)
		}
	}
}

// lock blocks until this process holds the machine's listener lock.
func (l *codexInbound) lock(ctx context.Context) (func(), error) {
	state := filepath.Dir(filepath.Dir(l.asks)) // .../state
	if err := os.MkdirAll(filepath.Join(state, "sock-integrations"), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(state, "sock-integrations", "codex-inbound.lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- syscall.Flock(int(f.Fd()), syscall.LOCK_EX) }()
	select {
	case err := <-done:
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		return func() { _ = f.Close() }, nil
	case <-ctx.Done():
		_ = f.Close() // the blocked flock returns with the file closed
		return nil, ctx.Err()
	}
}

// watch asks for a rescan when connected and whenever the daemon starts a
// thread, reconnecting with backoff when the daemon goes away.
func (l *codexInbound) watch(ctx context.Context, rescan chan<- struct{}) {
	poke := func() {
		select {
		case rescan <- struct{}{}:
		default:
		}
	}
	delay := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := l.daemon.Watch(ctx, poke)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute {
			delay = time.Second
		}
		l.log.Warn("Codex daemon connection ended, reconnecting", "in", delay, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, 30*time.Second)
	}
}

// rescan subscribes for every project with a loaded session and a cached
// puppet that has no subscription yet.
// A subscription outlives its sessions: a reply for a project with none
// loaded is left pending at the hub.
func (l *codexInbound) rescan(ctx context.Context) {
	projects, err := l.daemon.Projects(ctx)
	if err != nil {
		l.log.Warn("cannot list Codex sessions", "err", err)
		return
	}
	cache := &client.Projects{Dir: client.ProjectsDir(l.getenv, l.key)}
	for _, p := range projects {
		l.mu.Lock()
		started := l.serving[p]
		l.mu.Unlock()
		if started {
			continue
		}
		// Only a puppet that exists can have messaged the owner, so a
		// project without one has no reply to wait for; it is looked at
		// again when a turn ends.
		tok, err := cache.Cached(ctx, p)
		if err != nil || tok == "" {
			continue
		}
		l.mu.Lock()
		l.serving[p] = true
		l.mu.Unlock()
		go l.serve(ctx, p, tok)
	}
}

// serve subscribes as project's puppet until ctx ends or the hub refuses the
// token for good; the project is then free for a later rescan.
func (l *codexInbound) serve(ctx context.Context, project, token string) {
	defer func() {
		l.mu.Lock()
		delete(l.serving, project)
		l.mu.Unlock()
	}()
	cfg := l.key // the key's server and gate, the puppet's token
	cfg.Token = token
	c := sockclient.New(cfg, version)
	me, allow, err := allowlist(ctx, c, l.getenv)
	if err != nil {
		l.log.Warn("cannot listen for a Codex project", "project", project, "err", err)
		return
	}
	dir, err := channel.CountsDir(l.getenv, me)
	if err != nil {
		l.log.Warn("cannot listen for a Codex project", "project", project, "err", err)
		return
	}
	counts, err := channel.OpenCounts(dir, time.Now())
	if err != nil {
		l.log.Warn("cannot listen for a Codex project", "project", project, "err", err)
		return
	}
	ch := &channel.Channel{Client: c, Allow: allow, Counts: counts, Log: l.log.With("project", project), Push: l.push(project, c, counts)}
	if err := ch.Run(ctx); err != nil {
		l.log.Warn("stopped listening for a Codex project", "project", project, "err", err)
	}
}

// push puts one owner reply into a session of project. Accepted or already
// there: acknowledged. No session: left pending at the hub. The daemon
// refusing or gone: retried with backoff, then left pending. It never ends
// the stream.
func (l *codexInbound) push(project string, c *sockclient.Client, counts *channel.Counts) func(context.Context, channel.Event) error {
	return func(ctx context.Context, ev channel.Event) error {
		if ev.Meta["kind"] == "status" {
			return nil
		}
		conv, id := ev.Meta["conversation_id"], ev.Meta["message_id"]
		reply := codex.Reply{Project: project, Asked: asks.Read(l.asks, conv), MessageID: id, Prompt: codex.Prompt(conv, id, ev.Content)}
		delay := time.Second
		for attempt := range 5 {
			r := l.daemon.Inject(ctx, reply)
			switch r.Outcome {
			case codex.Accepted, codex.Duplicate:
				l.log.Info("reply put into a Codex session", "message_id", id, "outcome", r.Outcome.String(), "session", r.Session, "turn", r.Turn)
				if err := l.ack(ctx, c, id); err != nil {
					l.log.Warn("could not acknowledge; a redelivery is found in the session and acknowledged then", "message_id", id, "err", err)
					return nil
				}
				counts.Forget(id)
				return nil
			case codex.NoSession:
				l.log.Info("no loaded Codex session in the project; the reply waits at the hub", "message_id", id)
				return nil
			}
			l.log.Warn("Codex daemon did not take the reply", "message_id", id, "outcome", r.Outcome.String(), "reason", r.Reason, "attempt", attempt+1)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(delay):
			}
			delay *= 2
		}
		return nil
	}
}

func (l *codexInbound) ack(ctx context.Context, c *sockclient.Client, id string) error {
	_, err := sockclient.Call(ctx, c, func(ctx context.Context) (struct{}, error) {
		_, err := c.Messaging.Ack(ctx, connect.NewRequest(&sockv1.AckRequest{MessageIds: []string{id}}))
		return struct{}{}, err
	})
	return err
}

// errCodexInboundNeedsKey is why a server without a machine key does not
// listen: only the key turns a session's directory into its project's puppet.
var errCodexInboundNeedsKey = errors.New("--codex-inbound needs a machine key in the config file (sock machine-key use)")
