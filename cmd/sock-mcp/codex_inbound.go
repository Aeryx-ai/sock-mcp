package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"connectrpc.com/connect"

	client "github.com/aeryx-ai/sock-client"
	sockv1 "github.com/aeryx-ai/sock-contract/gen/go/sock/v1"

	"github.com/aeryx-ai/sock-mcp/internal/channel"
	"github.com/aeryx-ai/sock-mcp/internal/codex"
	"github.com/aeryx-ai/sock-mcp/sockclient"
)

// codexInbound is the Codex plugin server's listener. Of every such server on
// the machine, the one holding the lock listens: each cached Codex session
// puppet whose thread the daemon has loaded is unarchived, subscribed to and
// its owner's replies put into exactly that thread; one whose thread is not
// loaded is archived. The rest wait on the lock and take over when its
// session ends; the operating system frees a dead holder's lock.
type codexInbound struct {
	key      client.Config // the machine key: its server, and the archive calls
	sessions *client.SessionPuppets
	daemon   codex.Daemon
	getenv   func(string) string
	log      *slog.Logger

	mu      sync.Mutex
	serving map[string]*subscription // thread id -> its subscription
	// noArchive is set when the hub answers Unimplemented: it has no archive
	// calls yet, so none are made until the next start.
	noArchive bool
}

// subscription is one serve of a thread; its identity lets serve remove only
// its own entry when a stop and a start race.
type subscription struct{ cancel context.CancelFunc }

func newCodexInbound(key client.Config, sessions *client.SessionPuppets, getenv func(string) string, stderr io.Writer) *codexInbound {
	return &codexInbound{
		key:      key,
		sessions: sessions,
		daemon:   codex.Daemon{Socket: codex.DefaultSocket(getenv), Version: version},
		getenv:   getenv,
		log:      slog.New(slog.NewTextHandler(stderr, nil)).With("component", "codex-inbound"),
		serving:  map[string]*subscription{},
	}
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

// lock blocks until this process holds the machine's listener lock, beside
// the session puppet cache.
func (l *codexInbound) lock(ctx context.Context) (func(), error) {
	if !filepath.IsAbs(l.sessions.Dir) {
		// never under the working directory, as for the puppet cache itself
		return nil, errors.New("no home directory for the listener lock (set HOME or XDG_STATE_HOME)")
	}
	if err := os.MkdirAll(l.sessions.Dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(l.sessions.Dir, "codex-inbound.lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	return lockFile(ctx, f)
}

// watch asks for a rescan when connected and whenever a thread changes,
// reconnecting with backoff when the daemon goes away.
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

// rescan brings every cached Codex session puppet in line with its thread:
// loaded means unarchived, named for the thread and subscribed; not loaded
// means archived and not subscribed. Only a puppet that exists can have
// messaged the owner, so a thread without one is left alone.
func (l *codexInbound) rescan(ctx context.Context) {
	threads, err := l.daemon.Threads(ctx)
	if err != nil {
		l.log.Warn("cannot list Codex sessions", "err", err)
		return
	}
	loaded := map[string]codex.Thread{}
	for _, t := range threads {
		loaded[t.ID] = t
	}
	for _, s := range l.sessions.Sessions() {
		if s.Platform != client.Codex {
			continue
		}
		t, ok := loaded[s.ID]
		l.setArchived(ctx, s, !ok)
		if !ok {
			l.stop(s.ID)
			continue
		}
		if t.Name != "" {
			if err := l.sessions.SetName(ctx, s, t.Name); err != nil {
				l.log.Warn("cannot rename a Codex session's puppet", "thread", s.ID, "err", err)
			}
		}
		l.start(ctx, s)
	}
}

// setArchived archives or unarchives s when that is not already its state
// (kept in the cache, so a restart does not repeat it). A failure is retried at
// a later rescan; a hub without the call is not asked again until restart.
func (l *codexInbound) setArchived(ctx context.Context, s client.Session, archived bool) {
	l.mu.Lock()
	skip := l.noArchive
	l.mu.Unlock()
	if skip {
		return
	}
	err := l.sessions.SetArchived(ctx, s, archived)
	switch {
	case err == nil:
	case connect.CodeOf(err) == connect.CodeUnimplemented:
		l.mu.Lock()
		l.noArchive = true
		l.mu.Unlock()
		l.log.Warn("the hub cannot archive puppets yet; not asking again until restart")
	default:
		l.log.Warn("cannot set a Codex session puppet's archive state; retried at a later rescan", "thread", s.ID, "archived", archived, "err", client.Describe(err))
	}
}

func (l *codexInbound) start(ctx context.Context, s client.Session) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.serving[s.ID]; ok {
		return
	}
	sctx, cancel := context.WithCancel(ctx)
	sub := &subscription{cancel: cancel}
	l.serving[s.ID] = sub
	go l.serve(sctx, s, sub)
}

func (l *codexInbound) stop(threadID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if sub, ok := l.serving[threadID]; ok {
		sub.cancel()
		delete(l.serving, threadID)
	}
}

// serve subscribes as s's puppet until ctx ends or the hub refuses the token
// for good; the thread is then free for a later rescan.
func (l *codexInbound) serve(ctx context.Context, s client.Session, sub *subscription) {
	defer func() {
		l.mu.Lock()
		if l.serving[s.ID] == sub { // a later start's entry is not ours to remove
			delete(l.serving, s.ID)
		}
		l.mu.Unlock()
		sub.cancel()
	}()
	cfg := l.key // the key's server and gate, the puppet's token
	cfg.Token = l.sessions.Cached(s)
	if cfg.Token == "" {
		return
	}
	c := sockclient.New(cfg, version)
	me, allow, err := ownerAllowlist(ctx, c)
	if err != nil {
		l.log.Warn("cannot listen for a Codex session", "thread", s.ID, "err", err)
		return
	}
	dir, err := channel.CountsDir(l.getenv, me)
	if err != nil {
		l.log.Warn("cannot listen for a Codex session", "thread", s.ID, "err", err)
		return
	}
	counts, err := channel.OpenCounts(dir, time.Now())
	if err != nil {
		l.log.Warn("cannot listen for a Codex session", "thread", s.ID, "err", err)
		return
	}
	ch := &channel.Channel{Client: c, Allow: allow, Counts: counts, Log: l.log.With("thread", s.ID), Push: l.push(s.ID, c, counts)}
	if err := ch.Run(ctx); err != nil {
		l.log.Warn("stopped listening for a Codex session", "thread", s.ID, "err", err)
	}
}

// push puts one owner reply into the thread. Accepted or already there:
// acknowledged. Not loaded: left pending at the hub. The daemon refusing or
// gone: retried with backoff, then left pending. It never ends the stream.
func (l *codexInbound) push(threadID string, c *sockclient.Client, counts *channel.Counts) func(context.Context, channel.Event) error {
	return func(ctx context.Context, ev channel.Event) error {
		if ev.Meta["kind"] == "status" {
			return nil
		}
		conv, id := ev.Meta["conversation_id"], ev.Meta["message_id"]
		prompt := channel.OwnerPrompt(conv, id, ev.Content)
		delay := time.Second
		for attempt := range 5 {
			r := l.daemon.Inject(ctx, threadID, id, prompt)
			switch r.Outcome {
			case codex.Accepted, codex.Duplicate:
				l.log.Info("reply put into its Codex session", "message_id", id, "thread", threadID, "outcome", r.Outcome.String(), "turn", r.Turn)
				if err := ackOne(ctx, c, id); err != nil {
					l.log.Warn("could not acknowledge; a redelivery is found in the thread and acknowledged then", "message_id", id, "err", err)
					return nil
				}
				counts.Forget(id)
				return nil
			case codex.NotLoaded:
				l.log.Info("the Codex session is not loaded; the reply waits at the hub", "message_id", id, "thread", threadID)
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

// ackOne acknowledges one delivered message.
func ackOne(ctx context.Context, c *sockclient.Client, id string) error {
	_, err := sockclient.Call(ctx, c, func(ctx context.Context) (struct{}, error) {
		_, err := c.Messaging.Ack(ctx, connect.NewRequest(&sockv1.AckRequest{MessageIds: []string{id}}))
		return struct{}{}, err
	})
	return err
}

// errCodexInboundNeedsKey is why a server without a machine key does not
// listen: only the key makes and archives session puppets.
var errCodexInboundNeedsKey = errors.New("--codex-inbound needs a machine key in the config file (sock machine-key use)")
