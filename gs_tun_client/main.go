// gs_tun_client: local side. Accepts TCP (socks5 proxy) and tunnels it over
// a KCP session whose datagrams ride the GAS doPost bus (no UDP sockets).
//
//	./gs_tun_client --gs https://script.google.com/macros/s/.../exec \
//	  --token <TOKEN> --sv <SERVER_ID> -l 127.0.0.1:18080
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	kcp "github.com/xtaci/kcp-go"

	"gscript-tunnel/gasclient"
	"gscript-tunnel/gaspacket"
	"gscript-tunnel/kcpclient"
)

func main() {
	gsURL := flag.String("gs", "", "GAS /exec URL (required)")
	token := flag.String("token", "", "bus auth token (required)")
	sv := flag.String("sv", "", "target server id (from server's startup log; required when servers use ids)")
	localAddr := flag.String("l", ":12948", "local TCP listen addr (socks5 proxy)")
	mode := flag.String("mode", "fast", "fast3, fast2, fast, normal, manual")
	mtu := flag.Int("mtu", 1350, "MTU for KCP datagrams")
	sndwnd := flag.Int("sndwnd", 128, "send window (packets)")
	rcvwnd := flag.Int("rcvwnd", 512, "receive window (packets)")
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
	logFile := flag.String("log", "", "log file, default stderr")
	quiet := flag.Bool("quiet", false, "suppress stream open/close messages")
	cfgFile := flag.String("c", "", "JSON config file (overrides flags)")
	tfo := flag.Bool("tfo", false, "enable TCP fast open (default off: TFO blackholes on some paths)")
	snmplog := flag.String("snmplog", "", "snmp log file pattern")
	snmpperiod := flag.Int("snmpperiod", 60, "snmp collect period in seconds")
	flag.Parse()

	if *gsURL == "" || *token == "" {
		flag.Usage()
		os.Exit(2)
	}

	// No KCP-layer encryption: the bus already rides HTTPS (TLS) and is
	// token-authenticated, so a separate pre-shared key adds nothing.
	cfg := &kcpclient.Config{
		LocalAddr: *localAddr, Crypt: "none", Mode: *mode,
		MTU: *mtu, SndWnd: *sndwnd, RcvWnd: *rcvwnd,
		DataShard: *ds, ParityShard: *ps, DSCP: *dscp, NoComp: *nocomp,
		SockBuf: *sockbuf, StreamBuf: *streambuf, StreamBufEn: *streambufEn,
		BoostTimeout: *streamboost, MaxFrameSize: *maxframe, PipeBuf: *pipebuf,
		KeepAlive: *keepalive, KeepAliveMS: *keepalivems, KeepAliveTimeout: *keepaliveTimeout,
		Log: *logFile, SnmpLog: *snmplog, SnmpPeriod: *snmpperiod, Quiet: *quiet,
		Service: "socks5", TFO: *tfo, // fixed: this end is always a socks5 proxy
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
		cfg.Service = "socks5"
	}

	sess := gaspacket.SessForTokenAndServer(*token, *sv)
	gc := gasclient.New(*gsURL, *token, sess)

	ann, err := gaspacket.NewAnnouncer(gc)
	if err != nil {
		log.Fatalln("announcer:", err)
	}
	// Re-announce the current run every 2 min so a restarted server picks
	// up this client even if the original announcement expired (600s TTL).
	go func() {
		for range time.Tick(120 * time.Second) {
			if err := ann.Announce(); err != nil {
				log.Println("re-announce:", err)
			}
		}
	}()

	dialKCP := func(block kcp.BlockCrypt) (*kcp.UDPSession, error) {
		runID := gaspacket.NewRunID()
		ann.SetRunID(runID)
		for {
			if err := ann.Announce(); err == nil {
				break
			}
			log.Println("announce failed, retrying:", err)
			time.Sleep(2 * time.Second)
		}
		log.Println("announced run", runID)
		runGC := gasclient.New(*gsURL, *token,
			gaspacket.SessForRun(*token, *sv, runID))
		gp, err := gaspacket.Dial(runGC, "up", "down")
		if err != nil {
			return nil, err
		}
		time.Sleep(250 * time.Millisecond) // let the server build its listener
		if err := ann.Announce(); err != nil {
			log.Println("re-announce failed:", err)
		}
		// NOTE: raddr must resolve as UDP; the virtual conn ignores it.
		ksess, err := kcp.NewConn("127.0.0.1:9", block, cfg.DataShard, cfg.ParityShard, gp)
		if err != nil {
			gp.Close()
			return nil, err
		}
		return ksess, nil
	}

	fmt.Printf("server command:\n  ./gs_tun_server --gs %s --token <TOKEN>\n", *gsURL)
	fmt.Printf("(same --token as here; sess is derived from the token automatically)\n\n")

	if err := kcpclient.Run(cfg, dialKCP); err != nil {
		log.Fatalln(err)
	}
}
