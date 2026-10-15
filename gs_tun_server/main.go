// gs_tun_server: target side (e.g. a VPS). Serves KCP sessions whose
// datagrams ride the GAS doPost bus (no UDP sockets, no inbound ports).
// The built-in proxy is always on: the paired gs_tun_client acts as a
// socks5 proxy, this end dials whatever the client asks for.
//
//	./gs_tun_server --gs https://script.google.com/macros/s/.../exec \
//	  --token <TOKEN>
//
// Server prints its id at startup; clients select it with --sv <id>.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	kcp "github.com/xtaci/kcp-go"

	"gscript-tunnel/gasclient"
	"gscript-tunnel/gaspacket"
	"gscript-tunnel/kcpserver"
)

func main() {
	gsURL := flag.String("gs", "", "GAS /exec URL (required)")
	token := flag.String("token", "", "bus auth token (required)")
	target := flag.String("t", "127.0.0.1:12948", "target addr (unused in proxy mode, kept for compat)")
	mode := flag.String("mode", "fast", "fast3, fast2, fast, normal, manual")
	initialRTO := flag.Int("initial-rto", 5000, "initial KCP retransmission timeout in ms for GAS transport")
	mtu := flag.Int("mtu", 1350, "MTU for KCP datagrams")
	sndwnd := flag.Int("sndwnd", 1024, "send window (packets)")
	rcvwnd := flag.Int("rcvwnd", 1024, "receive window (packets)")
	ds := flag.Int("ds", 0, "FEC datashard (0 disables FEC for the reliable GAS/KCP path)")
	ps := flag.Int("ps", 0, "FEC parityshard (0 disables FEC for the reliable GAS/KCP path)")
	dscp := flag.Int("dscp", 0, "DSCP value")
	nocomp := flag.Bool("nocomp", false, "disable compression")
	sockbuf := flag.Int("sockbuf", 4*1024*1024, "per-session buffer in bytes")
	streambufEn := flag.Bool("streambuf-en", true, "enable per-socket buffer")
	streambuf := flag.Int("streambuf", 256*1024, "per-socket buffer in bytes")
	streamboost := flag.Int("streamboost", 10000, "stream boost for startup in ms")
	maxframe := flag.Int("maxframe", 32767, "max smux frame size in bytes")
	pipebuf := flag.Int("pipebuf", 256*1024, "internal io.CopyBuffer size in bytes")
	keepalive := flag.Int("keepalive", 10, "(deprecated) seconds between heartbeats")
	keepalivems := flag.Int("keepalivems", 10000, "milliseconds between heartbeats")
	keepaliveTimeout := flag.Int("keepalive-timeout", 75000, "heartbeat response timeout in ms")
	dns := flag.String("dns", "", "failback DNS, comma-separated")
	svID := flag.String("sv-id", "", "override auto-generated server id (for a fresh bus namespace)")
	logFile := flag.String("log", "", "log file, default stderr")
	quiet := flag.Bool("quiet", false, "suppress stream open/close messages")
	cfgFile := flag.String("c", "", "JSON config file (overrides flags)")
	tfo := flag.Bool("tfo", false, "enable TCP fast open (default off: TFO blackholes on some paths)")
	snmplog := flag.String("snmplog", "", "snmp log file pattern")
	snmpperiod := flag.Int("snmpperiod", 60, "snmp collect period in seconds")
	pprof := flag.Bool("pprof", false, "pprof on :6060")
	flag.Parse()

	if *gsURL == "" || *token == "" {
		flag.Usage()
		os.Exit(2)
	}

	// No KCP-layer encryption: the bus already rides HTTPS (TLS) and is
	// token-authenticated, so a separate pre-shared key adds nothing.
	cfg := &kcpserver.Config{
		Target: *target, Crypt: "none", Mode: *mode,
		MTU: *mtu, SndWnd: *sndwnd, RcvWnd: *rcvwnd,
		InitialRTO: *initialRTO,
		DataShard:  *ds, ParityShard: *ps, DSCP: *dscp, NoComp: *nocomp,
		SockBuf: *sockbuf, StreamBuf: *streambuf, StreamBufEn: *streambufEn,
		BoostTimeout: *streamboost, MaxFrameSize: *maxframe, PipeBuf: *pipebuf,
		KeepAlive: *keepalive, KeepAliveMS: *keepalivems, KeepAliveTimeout: *keepaliveTimeout,
		Log: *logFile, SnmpLog: *snmplog, SnmpPeriod: *snmpperiod,
		Pprof: *pprof, Quiet: *quiet,
		Service: "fast", TFO: *tfo, // fixed: built-in proxy always on
	}
	if *dns != "" {
		cfg.DNS = strings.Split(*dns, ",")
	}
	if *cfgFile != "" {
		f, err := os.Open(*cfgFile)
		if err != nil {
			log.Fatalln(err)
		}
		if err := json.NewDecoder(f).Decode(cfg); err != nil {
			log.Fatalln(err)
		}
		f.Close()
		cfg.Service = "fast"
	}

	// Multi-server: each server gets a stable ID derived from its hostname,
	// and the bus namespace includes it, so one token can serve many servers.
	// --sv-id overrides the auto-generated ID (e.g. to escape a polluted bus).
	serverID := gaspacket.ServerID(*token)
	if *svID != "" {
		serverID = *svID
	}
	sess := gaspacket.SessForTokenAndServer(*token, serverID)
	fmt.Printf("server id: %s  (clients use --sv %s)\n", serverID, serverID)
	gc := gasclient.New(*gsURL, *token, sess)

	// One handler per client run. A new run ID means the client built a new
	// KCP session (new conv); the old listener is torn down so its stale
	// session entry can never blackhole the new conv. The announce stream
	// doubles as a heartbeat: a run not re-announced for 5 minutes is
	// considered gone and its handler is reaped.
	runCh := gaspacket.WatchRuns(gc)
	var (
		curRun   string
		stopCh   chan struct{}
		lastSeen time.Time
	)
	startHandler := func(runID string) {
		if stopCh != nil {
			close(stopCh)
			// give the old handler a moment to release its bus conn
			time.Sleep(500 * time.Millisecond)
		}
		curRun = runID
		stopCh = make(chan struct{})
		rid := runID
		runGC := gasclient.New(*gsURL, *token,
			gaspacket.SessForRun(*token, serverID, runID))
		listenKCP := func(block kcp.BlockCrypt) (*kcp.Listener, func(), error) {
			gp, err := gaspacket.Dial(runGC, "down", "up")
			if err != nil {
				return nil, nil, err
			}
			lis, err := kcp.ServeConn(block, cfg.DataShard, cfg.ParityShard, gp)
			if err != nil {
				gp.Close()
				return nil, nil, err
			}
			return lis, func() { gp.Close() }, nil
		}
		log.Printf("client run %s: starting KCP listener", rid)
		go func() {
			if err := kcpserver.Run(cfg, listenKCP, stopCh); err != nil {
				log.Printf("run %s handler ended: %v", rid, err)
			} else {
				log.Printf("run %s handler ended", rid)
			}
		}()
	}
	reapTick := time.NewTicker(60 * time.Second)
	defer reapTick.Stop()
	for {
		select {
		case runID := <-runCh:
			lastSeen = time.Now()
			if runID == curRun {
				continue
			}
			startHandler(runID)
		case <-reapTick.C:
			if curRun != "" && time.Since(lastSeen) > 5*time.Minute {
				log.Printf("run %s silent for 5m, tearing down", curRun)
				close(stopCh)
				stopCh = nil
				curRun = ""
			}
		}
	}
}
