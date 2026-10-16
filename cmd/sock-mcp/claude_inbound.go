package main

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	client "github.com/guygrigsby/sock-client"

	"github.com/guygrigsby/sock-mcp/internal/channel"
	"github.com/guygrigsby/sock-mcp/internal/inbox"
	"github.com/guygrigsby/sock-mcp/sockclient"
)

// claudeSession is a Claude Code plugin server with --claude-inbound
// (sock-integrations ADR 3 and ADR 4). With a machine key every call runs as
// the session's own puppet, made on its first call; without one, as the
// project's puppet. Once that puppet exists, the owner's replies to it are
// written into the session's inbox and acknowledged.
type claudeSession struct {
	key      client.Config
	sessions *client.SessionPuppets // nil: no machine key, one fixed puppet
	session  client.Session
	dir      string // the project, for the name of a session not yet named
	inbox    inbox.Inbox
	getenv   func(string) string
	log      *slog.Logger

	mu     sync.Mutex
	c      *sockclient.Client
	ready  chan struct{} // closed once c is set
	title  titleReader
	noArch bool // the hub answered Unimplemented to an archive call
}

func newClaudeSession(key client.Config, sessions *client.SessionPuppets, id string, getenv func(string) string, stderr io.Writer) *claudeSession {
	s := &claudeSession{
		key:      key,
		sessions: sessions,
		session:  client.Session{Platform: client.ClaudeCode, ID: id},
		dir:      cmp.Or(getenv("CLAUDE_PROJECT_DIR"), workingDir()),
		getenv:   getenv,
		log:      slog.New(slog.NewTextHandler(stderr, nil)).With("component", "claude-inbound", "session", id),
		ready:    make(chan struct{}),
		title:    titleReader{path: transcriptPath(getenv, id)},
	}
	s.inbox, _ = inbox.FromEnv(getenv)
	if sessions != nil {
		if tok := sessions.Cached(s.session); tok != "" {
			s.setClient(tok)
		}
	}
	return s
}

// fixed makes c the session's client: the project's puppet, when there is no
// machine key to make the session its own.
func (s *claudeSession) fixed(c *sockclient.Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.c = c
	close(s.ready)
}

func (s *claudeSession) setClient(tok string) {
	cfg := s.key // the key's server and gate, the puppet's token
	cfg.Token = tok
	s.c = sockclient.New(cfg, version)
	close(s.ready)
}

// For is the client of every tool call: the session's puppet, made with the
// machine key on the first call and renamed whenever the session's title
// changed since.
func (s *claudeSession) For(ctx context.Context, _ *mcp.CallToolRequest) (*sockclient.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.c != nil {
		s.rename(ctx)
		return s.c, nil
	}
	name := cmp.Or(s.title.Title(), fallbackName(s.dir, s.session.ID))
	tok, err := s.sessions.Ensure(ctx, s.session, name)
	if err != nil {
		return nil, err
	}
	s.setClient(tok)
	return s.c, nil
}

// rename sets the puppet's name to the session's title, once the owner gave
// it one (/rename). A session never titled keeps the name it was made with.
func (s *claudeSession) rename(ctx context.Context) {
	if s.sessions == nil {
		return
	}
	if t := s.title.Title(); t != "" {
		if err := s.sessions.SetName(ctx, s.session, t); err != nil {
			s.log.Warn("cannot rename the session's puppet", "err", err)
		}
	}
}

// Run listens until ctx ends, then archives the session's puppet. With a
// machine key it first holds the session's run lock, so one server of a
// session listens at a time, and archives the puppets of sessions whose
// server died without doing so.
func (s *claudeSession) Run(ctx context.Context) {
	if s.sessions != nil {
		unlock, err := runLock(ctx, s.sessions.Dir, s.session)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Error("no inbound: cannot take the session's run lock", "err", err)
			}
			return
		}
		defer unlock()
		s.reconcile(ctx)
	}
	select {
	case <-ctx.Done():
		return
	case <-s.ready:
	}
	if s.sessions != nil {
		s.setArchived(ctx, false)
		defer func() {
			actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			s.setArchived(actx, true)
		}()
		s.mu.Lock()
		s.rename(ctx)
		s.mu.Unlock()
	}
	s.listen(ctx)
}

// listen subscribes as the session's puppet and puts each of its owner's
// replies into the session.
func (s *claudeSession) listen(ctx context.Context) {
	s.mu.Lock()
	c := s.c
	s.mu.Unlock()
	me, allow, err := allowlist(ctx, c, s.getenv)
	if err != nil {
		s.log.Warn("no inbound", "err", err)
		return
	}
	dir, err := channel.CountsDir(s.getenv, me)
	if err != nil {
		s.log.Warn("no inbound", "err", err)
		return
	}
	counts, err := channel.OpenCounts(dir, time.Now())
	if err != nil {
		s.log.Warn("no inbound", "err", err)
		return
	}
	s.log.Info("listening for the owner's replies")
	ch := &channel.Channel{Client: c, Allow: allow, Counts: counts, Log: s.log, Push: s.push(c, counts)}
	if err := ch.Run(ctx); err != nil {
		s.log.Warn("stopped listening; the tools still work", "err", err)
	}
}

// push writes one owner reply into the session's inbox and acknowledges it
// once the inbox took it. An inbox that refuses is retried with backoff, then
// the reply is left pending at the hub. It never ends the stream.
func (s *claudeSession) push(c *sockclient.Client, counts *channel.Counts) func(context.Context, channel.Event) error {
	return func(ctx context.Context, ev channel.Event) error {
		if ev.Meta["kind"] == "status" {
			return nil
		}
		id := ev.Meta["message_id"]
		prompt := channel.OwnerPrompt(ev.Meta["conversation_id"], id, ev.Content)
		delay := time.Second
		for attempt := range 5 {
			err := s.inbox.Send(ctx, prompt)
			if err == nil {
				s.log.Info("reply put into the session", "message_id", id)
				if err := ackOne(ctx, c, id); err != nil {
					s.log.Warn("could not acknowledge; a redelivery arrives as a repeat", "message_id", id, "err", err)
					return nil
				}
				counts.Forget(id)
				return nil
			}
			s.log.Warn("the session's inbox did not take the reply", "message_id", id, "attempt", attempt+1, "err", err)
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

// reconcile archives every other Claude Code session puppet whose run lock is
// free: its server ended without archiving (crashed, killed).
func (s *claudeSession) reconcile(ctx context.Context) {
	for _, other := range s.sessions.Sessions() {
		if other.Platform != client.ClaudeCode || other == s.session {
			continue
		}
		unlock, held := tryRunLock(s.sessions.Dir, other)
		if held {
			continue
		}
		s.setArchivedOf(ctx, other, true)
		unlock()
	}
}

func (s *claudeSession) setArchived(ctx context.Context, archived bool) {
	s.setArchivedOf(ctx, s.session, archived)
}

// setArchivedOf archives or unarchives when that is not already the state. A
// failure is retried at the next start; a hub without the call is not asked
// again by this server.
func (s *claudeSession) setArchivedOf(ctx context.Context, sess client.Session, archived bool) {
	s.mu.Lock()
	skip := s.noArch
	s.mu.Unlock()
	if skip {
		return
	}
	err := s.sessions.SetArchived(ctx, sess, archived)
	switch {
	case err == nil:
	case connect.CodeOf(err) == connect.CodeUnimplemented:
		s.mu.Lock()
		s.noArch = true
		s.mu.Unlock()
		s.log.Warn("the hub cannot archive puppets yet; not asking again")
	default:
		s.log.Warn("cannot set a session puppet's archive state; retried at a later start", "of", sess.ID, "archived", archived, "err", client.Describe(err))
	}
}

// runLockPath is where a session's server holds its run lock.
func runLockPath(dir string, sess client.Session) string {
	return filepath.Join(dir, "run", string(sess.Platform)+"-"+sess.ID+".lock")
}

func openRunLock(dir string, sess client.Session) (*os.File, error) {
	if !filepath.IsAbs(dir) {
		// never under the working directory, as for the puppet cache itself
		return nil, errors.New("no home directory for the run lock (set HOME or XDG_STATE_HOME)")
	}
	if err := os.MkdirAll(filepath.Join(dir, "run"), 0o700); err != nil {
		return nil, err
	}
	return os.OpenFile(runLockPath(dir, sess), os.O_RDWR|os.O_CREATE, 0o600)
}

// runLock blocks until this process holds sess's run lock. The operating
// system frees a dead holder's lock.
func runLock(ctx context.Context, dir string, sess client.Session) (func(), error) {
	f, err := openRunLock(dir, sess)
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

// tryRunLock takes sess's run lock without waiting. held reports that a
// running server has it; otherwise unlock releases what this took.
func tryRunLock(dir string, sess client.Session) (unlock func(), held bool) {
	f, err := openRunLock(dir, sess)
	if err != nil {
		return nil, true // unknown: leave it be
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, true
	}
	return func() { _ = f.Close() }, false
}

// transcriptPath is the session's transcript, found by its id under
// ${CLAUDE_CONFIG_DIR:-~/.claude}/projects, or "" when there is none yet.
func transcriptPath(getenv func(string) string, id string) string {
	base := cmp.Or(getenv("CLAUDE_CONFIG_DIR"), filepath.Join(getenv("HOME"), ".claude"))
	// id is [A-Za-z0-9_-]: no glob metacharacters
	m, _ := filepath.Glob(filepath.Join(base, "projects", "*", id+".jsonl"))
	if len(m) == 0 {
		return ""
	}
	return m[0]
}

// titleReader follows a transcript for the session's title: the last
// custom-title entry (/rename). It reads only what was appended since the
// last call.
type titleReader struct {
	path  string
	off   int64
	title string
}

func (r *titleReader) Title() string {
	if r.path == "" {
		return ""
	}
	f, err := os.Open(r.path)
	if err != nil {
		return r.title
	}
	defer func() { _ = f.Close() }()
	if fi, err := f.Stat(); err != nil || fi.Size() < r.off {
		r.off = 0 // replaced: read it again
	}
	if _, err := f.Seek(r.off, io.SeekStart); err != nil {
		return r.title
	}
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			break // a partial last line is read again next time
		}
		r.off += int64(len(line))
		if !bytes.Contains(line, []byte(`"custom-title"`)) {
			continue
		}
		var e struct {
			Type  string `json:"type"`
			Title string `json:"customTitle"`
		}
		if json.Unmarshal(line, &e) == nil && e.Type == "custom-title" && e.Title != "" {
			r.title = e.Title
		}
	}
	return r.title
}

func workingDir() string {
	d, _ := os.Getwd()
	return d
}
