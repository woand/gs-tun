package kcpserver

import (
	"crypto/sha1"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"
	_ "unsafe"

	"golang.org/x/crypto/pbkdf2"

	"path/filepath"

	"github.com/cs8425/smux"
	"github.com/golang/snappy"
	kcp "github.com/xtaci/kcp-go"
)

var (
	// VERSION is injected by buildflags
	VERSION = "SELFBUILD"
	// SALT is use for pbkdf2 key expansion
	SALT = "kcp-go"
)

type compStream struct {
	conn net.Conn
	w    *snappy.Writer
	r    *snappy.Reader
}

func (c *compStream) Read(p []byte) (n int, err error) {
	return c.r.Read(p)
}

func (c *compStream) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	err = c.w.Flush()
	return n, err
}

func (c *compStream) Close() error {
	return c.conn.Close()
}

func newCompStream(conn net.Conn) *compStream {
	c := new(compStream)
	c.conn = conn
	c.w = snappy.NewBufferedWriter(conn)
	c.r = snappy.NewReader(conn)
	return c
}

// DNS resolve workaround for android in pure go

//go:linkname defaultNS net.defaultNS
var defaultNS []string

func setDefaultNS(addrs []string) {
	defaultNS = addrs
}

// handle multiplex-ed connection
func handleMux(conn io.ReadWriteCloser, config *Config) {
	// stream multiplex
	smuxConfig := smux.DefaultConfig()
	smuxConfig.MaxReceiveBuffer = config.SockBuf
	smuxConfig.KeepAliveInterval = time.Duration(config.KeepAliveMS) * time.Millisecond
	smuxConfig.KeepAliveTimeout = time.Duration(config.KeepAliveTimeout) * time.Millisecond
	smuxConfig.MaxFrameSize = config.MaxFrameSize
	smuxConfig.MaxStreamBuffer = config.StreamBuf
	smuxConfig.EnableStreamBuffer = config.StreamBufEn
	smuxConfig.BoostTimeout = time.Duration(config.BoostTimeout) * time.Millisecond

	mux, err := smux.Server(conn, smuxConfig)
	if err != nil {
		log.Println(err)
		return
	}
	defer mux.Close()
	for {
		p1, err := mux.AcceptStream()
		if err != nil {
			log.Println(err)
			return
		}

		go handleClient(p1, config.Quiet, config.PipeBuf, config.Service, config.Target, config.TFO)
	}
}

// Run starts the kcptunB server for one client run. listenKCP creates the
// KCP listener over the caller-supplied packet connection (e.g. the GAS
// bus) and returns a cleanup func for it. Closing stop tears everything
// down so the caller can start a fresh run (new client conv).
func Run(config *Config, listenKCP func(block kcp.BlockCrypt) (lis *kcp.Listener, cleanup func(), err error), stop <-chan struct{}) error {
	rand.Seed(int64(time.Now().Nanosecond()))
	if VERSION == "SELFBUILD" {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}
	// log redirect
	if config.Log != "" {
		f, err := os.OpenFile(config.Log, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		log.SetOutput(f)
	}

	switch config.Mode {
	case "normal":
		config.NoDelay, config.Interval, config.Resend, config.NoCongestion = 0, 40, 2, 1
	case "fast":
		config.NoDelay, config.Interval, config.Resend, config.NoCongestion = 0, 30, 2, 1
	case "fast2":
		config.NoDelay, config.Interval, config.Resend, config.NoCongestion = 1, 20, 2, 1
	case "fast3":
		config.NoDelay, config.Interval, config.Resend, config.NoCongestion = 1, 10, 2, 1
	}

	log.Println("version:", VERSION)
	log.Println("initiating key derivation")
	pass := pbkdf2.Key([]byte(config.Key), []byte(SALT), 4096, 32, sha1.New)
	var block kcp.BlockCrypt
	switch config.Crypt {
	case "sm4":
		block, _ = kcp.NewSM4BlockCrypt(pass[:16])
	case "tea":
		block, _ = kcp.NewTEABlockCrypt(pass[:16])
	case "xor":
		block, _ = kcp.NewSimpleXORBlockCrypt(pass)
	case "none":
		block, _ = kcp.NewNoneBlockCrypt(pass)
	case "aes-128":
		block, _ = kcp.NewAESBlockCrypt(pass[:16])
	case "aes-192":
		block, _ = kcp.NewAESBlockCrypt(pass[:24])
	case "blowfish":
		block, _ = kcp.NewBlowfishBlockCrypt(pass)
	case "twofish":
		block, _ = kcp.NewTwofishBlockCrypt(pass)
	case "cast5":
		block, _ = kcp.NewCast5BlockCrypt(pass[:16])
	case "3des":
		block, _ = kcp.NewTripleDESBlockCrypt(pass[:24])
	case "xtea":
		block, _ = kcp.NewXTEABlockCrypt(pass[:16])
	case "salsa20":
		block, _ = kcp.NewSalsa20BlockCrypt(pass)
	default:
		config.Crypt = "aes"
		block, _ = kcp.NewAESBlockCrypt(pass)
	}

	if config.KeepAliveMS == 0 {
		config.KeepAliveMS = config.KeepAlive * 1000
	}

	lis, cleanup, err := listenKCP(block)
	if err != nil {
		return err
	}
	defer cleanup()
	go func() {
		<-stop
		lis.Close() // unblocks AcceptKCP; also closes the bus conn
	}()
	log.Println("listening on:", lis.Addr())
	log.Println("target:", config.Target)
	log.Println("encryption:", config.Crypt)
	log.Println("nodelay parameters:", config.NoDelay, config.Interval, config.Resend, config.NoCongestion)
	log.Println("sndwnd:", config.SndWnd, "rcvwnd:", config.RcvWnd)
	log.Println("compression:", !config.NoComp)
	log.Println("mtu:", config.MTU)
	log.Println("datashard:", config.DataShard, "parityshard:", config.ParityShard)
	log.Println("acknodelay:", config.AckNodelay)
	log.Println("dscp:", config.DSCP)
	log.Println("sockbuf:", config.SockBuf)
	log.Println("snmplog:", config.SnmpLog)
	log.Println("snmpperiod:", config.SnmpPeriod)
	log.Println("pprof:", config.Pprof)
	log.Println("quiet:", config.Quiet)

	log.Println("keepaliveMS:", config.KeepAliveMS)
	log.Println("keepalive-timeout:", config.KeepAliveTimeout)
	log.Println("sockbuf:", config.SockBuf)
	log.Println("streambuf-en:", config.StreamBufEn)
	log.Println("streambuf:", config.StreamBuf)
	log.Println("boosttimeout:", config.BoostTimeout)
	log.Println("maxframe:", config.MaxFrameSize)
	log.Println("pipebuf:", config.PipeBuf)

	log.Println("service:", config.Service)
	log.Println("TCP Fast Open(tfo):", config.TFO)

	if len(config.DNS) > 0 {
		log.Println("failback-DNS:", config.DNS)
		setDefaultNS(config.DNS)
	}

	if err := lis.SetDSCP(config.DSCP); err != nil {
		log.Println("SetDSCP:", err)
	}
	if err := lis.SetReadBuffer(config.SockBuf); err != nil {
		log.Println("SetReadBuffer:", err)
	}
	if err := lis.SetWriteBuffer(config.SockBuf); err != nil {
		log.Println("SetWriteBuffer:", err)
	}

	go snmpLogger(config.SnmpLog, config.SnmpPeriod)
	if config.Pprof {
		go http.ListenAndServe(":6060", nil)
	}

	for {
		if conn, err := lis.AcceptKCP(); err == nil {
			log.Println("remote address:", conn.RemoteAddr())
			conn.SetStreamMode(true)
			conn.SetWriteDelay(false)
			conn.SetNoDelay(config.NoDelay, config.Interval, config.Resend, config.NoCongestion)
			if config.InitialRTO > 0 {
				conn.SetInitialRTO(config.InitialRTO)
			}
			conn.SetMtu(config.MTU)
			conn.SetWindowSize(config.SndWnd, config.RcvWnd)
			conn.SetACKNoDelay(config.AckNodelay)

			if config.NoComp {
				go handleMux(conn, config)
			} else {
				go handleMux(newCompStream(conn), config)
			}
		} else {
			select {
			case <-stop:
				return nil
			default:
				log.Printf("%+v", err)
				time.Sleep(time.Second)
			}
		}
	}
}

func snmpLogger(path string, interval int) {
	if path == "" || interval == 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// split path into dirname and filename
			logdir, logfile := filepath.Split(path)
			// only format logfile
			f, err := os.OpenFile(logdir+time.Now().Format(logfile), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				log.Println(err)
				return
			}
			w := csv.NewWriter(f)
			// write header in empty file
			if stat, err := f.Stat(); err == nil && stat.Size() == 0 {
				if err := w.Write(append([]string{"Unix"}, kcp.DefaultSnmp.Header()...)); err != nil {
					log.Println(err)
				}
			}
			if err := w.Write(append([]string{fmt.Sprint(time.Now().Unix())}, kcp.DefaultSnmp.ToSlice()...)); err != nil {
				log.Println(err)
			}
			kcp.DefaultSnmp.Reset()
			w.Flush()
			f.Close()
		}
	}
}
