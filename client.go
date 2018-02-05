package gcm

import (
	"errors"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

// httpC is an interface to stub the internal HTTP client.
type httpC interface {
	Send(m HTTPMessage) (*HTTPResponse, error)
}

// xmppC is an interface to stub the internal XMPP client.
type xmppC interface {
	Listen(h MessageHandler) error
	Send(m XMPPMessage) (string, int, error)
	Ping(timeout time.Duration) error
	Close(graceful bool) error
	IsClosed() bool
	ID() string
	JID() string
}

// gcmClient is a container for http and xmpp GCM clients.
type gcmClient struct {
	sync.RWMutex
	mh        MessageHandler
	cerr      chan error
	sandbox   bool
	fcm       bool
	debug     bool
	omitRetry bool
	// Clients.
	xmppClient xmppC
	httpClient httpC

	// Synchronize sending xmpp with the replacement of xmppClient through xmppChan
	xmppChan chan xmppPacket

	// GCM auth.
	senderID string
	apiKey   string
	// XMPP config.
	pingInterval time.Duration
	pingTimeout  time.Duration
	killMonitor  chan bool
}

// NewClient creates a new GCM client for these credentials.
func NewClient(config *Config, h MessageHandler) (Client, error) {
	switch {
	case config == nil:
		return nil, errors.New("config is nil")
	case h == nil:
		return nil, errors.New("message handler is nil")
	case config.APIKey == "":
		return nil, errors.New("empty api key")
	}

	useHTTPOnly := config.SenderID == ""

	// Create GCM HTTP client.
	httpc := newHTTPClient(
		config.APIKey,
		config.Debug,
		config.OmitInternalRetry,
		config.HTTPTimeout,
		config.HTTPTransport,
	)

	var xmppc xmppC
	var err error
	if !useHTTPOnly {
		// Create GCM XMPP client.
		xmppc, err = newXMPPClient(config.Sandbox, config.UseFCM, config.SenderID, config.APIKey, config.Debug, config.OmitInternalRetry)
		if err != nil {
			return nil, err
		}
	}

	// Construct GCM client.
	return newGCMClient(xmppc, httpc, config, h)
}

// ID returns client unique identification.
func (c *gcmClient) ID() string {
	c.RLock()
	defer c.RUnlock()
	return c.xmppClient.ID()
}

// JID returns client XMPP JID.
func (c *gcmClient) JID() string {
	c.RLock()
	defer c.RUnlock()
	return c.xmppClient.JID()
}

// SendHTTP sends a message using the HTTP GCM connection server (blocking).
func (c *gcmClient) SendHTTP(m HTTPMessage) (*HTTPResponse, error) {
	return c.httpClient.Send(m)
}

type xmppResponse struct {
	MessageID string
	Bytes     int
	Err       error
}
type xmppPacket struct {
	m  XMPPMessage
	rc chan xmppResponse
}

// SendXMPP sends a message using the XMPP GCM connection server (blocking).
func (c *gcmClient) SendXMPP(m XMPPMessage) (string, int, error) {
	rc := make(chan xmppResponse)
	c.xmppChan <- xmppPacket{m, rc}
	resp := <-rc
	return resp.MessageID, resp.Bytes, resp.Err
}

// Close will stop and close the corresponding client, releasing all resources (blocking).
func (c *gcmClient) Close() (err error) {
	c.Lock()
	defer c.Unlock()

	close(c.killMonitor)
	if c.xmppClient != nil {
		err = c.xmppClient.Close(true)
	}

	return
}

// newGCMClient creates an instance of gcmClient.
func newGCMClient(xmppc xmppC, httpc httpC, config *Config, h MessageHandler) (*gcmClient, error) {
	c := &gcmClient{
		httpClient:   httpc,
		xmppClient:   xmppc,
		xmppChan:     make(chan xmppPacket),
		cerr:         make(chan error, 1),
		senderID:     config.SenderID,
		apiKey:       config.APIKey,
		mh:           h,
		debug:        config.Debug,
		omitRetry:    config.OmitInternalRetry,
		sandbox:      config.Sandbox,
		fcm:          config.UseFCM,
		pingInterval: time.Duration(config.PingInterval) * time.Second,
		pingTimeout:  time.Duration(config.PingTimeout) * time.Second,
		killMonitor:  make(chan bool, 1),
	}
	if c.pingInterval <= 0 {
		c.pingInterval = DefaultPingInterval
	}
	if c.pingTimeout <= 0 {
		c.pingTimeout = DefaultPingTimeout
	}

	clientIsConnected := make(chan bool, 1)
	if xmppc != nil {
		// Create and monitor XMPP client.
		go c.monitorXMPP(config.MonitorConnection, clientIsConnected)

		select {
		case err := <-c.cerr:
			c.killMonitor <- true
			return nil, err
		case <-clientIsConnected:
			return c, nil
		case <-time.After(10 * time.Second):
			c.killMonitor <- true
			return nil, errors.New("Timed out attempting to connect client")
		}
	} else {
		return c, nil
	}
}

func (c *gcmClient) loop(activeMonitor bool) {
	for {
		select {
		case <-c.killMonitor:
			return
		case packet := <-c.xmppChan:
			c.RLock()
			r := xmppResponse{}
			r.MessageID, r.Bytes, r.Err = c.xmppClient.Send(packet.m)
			packet.rc <- r
			c.RUnlock()
		case err := <-c.cerr:
			if err == nil {
				// No error, active close.
				return
			}

			log.WithField("xmpp client ref", c.xmppClient.ID()).WithField("error", err).Error("gcm xmpp connection")

			c.Lock()
			c.replaceXMPPClient(activeMonitor)
			c.Unlock()
		}
	}
}

// Replace the active client.
func (c *gcmClient) replaceXMPPClient(activeMonitor bool) {
	prevc := c.xmppClient
	c.cerr = make(chan error)
	xmppc, err := connectXMPP(nil, c.sandbox, c.fcm, c.senderID, c.apiKey,
		c.onCCSMessage, c.cerr, c.debug, c.omitRetry)
	if err != nil {
		log.WithFields(log.Fields{"sender id": c.senderID, "error": err}).
			Error("connect gcm xmpp client")
		// Wait and try again.
		// TODO: remove infinite loop.
		time.Sleep(c.pingTimeout)
		c.replaceXMPPClient(activeMonitor)
	} else {
		c.xmppClient = xmppc
		// Close the previous client
		go prevc.Close(true)

		log.WithField("xmpp client ref", xmppc.ID()).WithField("previous xmpp client ref", prevc.ID()).
			Warn("gcm xmpp client replaced")

		if activeMonitor {
			c.spinUpActiveMonitor()
		}
	}
}

func (c *gcmClient) spinUpActiveMonitor() {
	go func(xc xmppC, ce chan<- error) {
		// pingPeriodically is blocking.
		perr := pingPeriodically(xc, c.pingTimeout, c.pingInterval)
		if !xc.IsClosed() {
			ce <- perr
		}
	}(c.xmppClient, c.cerr)
	log.WithField("xmpp client ref", c.xmppClient.ID()).Debug("gcm xmpp connection monitoring started")
}

// monitorXMPP creates a new GCM XMPP client (if not provided), replaces the active client,
// closes the old client and starts monitoring the new connection.
func (c *gcmClient) monitorXMPP(activeMonitor bool, clientIsConnected chan bool) {
	// Create XMPP client.
	log.WithField("sender id", c.senderID).Debug("creating gcm xmpp client")
	xmppc, err := connectXMPP(c.xmppClient, c.sandbox, c.fcm, c.senderID, c.apiKey,
		c.onCCSMessage, c.cerr, c.debug, c.omitRetry)
	if err != nil {
		// On initial connection, error exits the monitor.
		return
	}
	log.WithField("xmpp client ref", xmppc.ID()).Info("gcm xmpp client created")

	// If active monitoring is enabled, start pinging routine.
	if activeMonitor {
		c.spinUpActiveMonitor()
	}

	go c.loop(activeMonitor)

	// Wait just a tick to ensure Listen got called - without this there's probably an edge-case where if the
	// threading happens exactly wrong you can create a client, return it, and push out a send before you start
	// listening for its response and therefore you miss the response.  Given network latency that would probably
	// not ever happen but just to be paranoid... this also ensures that the tests (which assert that Listen got
	// called) reliably pass.
	time.Sleep(time.Millisecond)
	clientIsConnected <- true
}

// CCS upstream message callback.
// Tries to handle what it can here, before bubbling up.
func (c *gcmClient) onCCSMessage(cm CCSMessage) error {
	switch cm.MessageType {
	case CCSControl:
		// Handle connection drainging request.
		if cm.ControlType == CCSDraining {
			log.WithField("xmpp client ref", c.xmppClient.ID()).
				Warn("gcm xmpp connection draining requested")
			// Server should close the current connection.
			c.Lock()
			cerr := c.cerr
			c.Unlock()
			cerr <- errors.New("connection draining")
		}
		// Don't bubble up control messages.
		return nil
	}
	// Bubble up everything else.
	return c.mh(cm)
}

// Creates a new xmpp client (if not provided), connects to the server and starts listening.
func connectXMPP(c xmppC, isSandbox bool, useFCM bool, senderID string, apiKey string,
	h MessageHandler, cerr chan<- error, debug bool, omitRetry bool) (xmppC, error) {
	var xmppc xmppC
	if c != nil {
		// Use the provided client.
		xmppc = c
	} else {
		// Create new.
		var err error
		xmppc, err = newXMPPClient(isSandbox, useFCM, senderID, apiKey, debug, omitRetry)
		if err != nil {
			cerr <- err
			return nil, err
		}
	}

	l := log.WithField("xmpp client ref", xmppc.ID())

	// Start listening on this connection.
	go func() {
		l.Debug("gcm xmpp listen started")
		if err := xmppc.Listen(h); err != nil {
			l.WithField("error", err).Error("gcm xmpp listen")
			cerr <- err
		}
		l.Debug("gcm xmpp listen finished")
	}()

	return xmppc, nil
}

// pingPeriodically sends periodic pings. If pong is received, the timer is reset.
func pingPeriodically(xm xmppC, timeout, interval time.Duration) error {
	t := time.NewTimer(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			if xm.IsClosed() {
				return nil
			}
			if err := xm.Ping(timeout); err != nil {
				return err
			}
			t.Reset(interval)
		}
	}
}
