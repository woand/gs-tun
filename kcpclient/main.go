package kcpclient

import (
	"crypto/sha1"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	"golang.org/x/crypto/pbkdf2"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
	kcp "github.com/xtaci/kcp-go"
	"github.com/cs8425/smux"

	"path/filepath"
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

// Run starts the kcptunB client. dialKCP creates the KCP session over
// the caller-supplied packet connection (e.g. the GAS bus).
func Run(config *Config, dialKCP func(block kcp.BlockCrypt) (*kcp.UDPSession, error)) error {
	rand.Seed(int64(time.Now().Nanosecond()))
	if VERSION == "SELFBUILD" {
		// add more log flags for debugging
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
		addr, err := net.ResolveTCPAddr("tcp", config.LocalAddr)
		if err != nil {
			return err
		}
		listener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return err
		}

		if config.TFO {
			bindTFO(listener)
		}

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

		log.Println("listening on:", listener.Addr())
		log.Println("encryption:", config.Crypt)
		log.Println("nodelay parameters:", config.NoDelay, config.Interval, config.Resend, config.NoCongestion)
		log.Println("sndwnd:", config.SndWnd, "rcvwnd:", config.RcvWnd)
		log.Println("compression:", !config.NoComp)
		log.Println("mtu:", config.MTU)
		log.Println("datashard:", config.DataShard, "parityshard:", config.ParityShard)
		log.Println("acknodelay:", config.AckNodelay)
		log.Println("dscp:", config.DSCP)
		log.Println("conn:", config.Conn)
		log.Println("autoexpire:", config.AutoExpire)
		log.Println("scavengettl:", config.ScavengeTTL)
		log.Println("snmplog:", config.SnmpLog)
		log.Println("snmpperiod:", config.SnmpPeriod)
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


		smuxConfig := smux.DefaultConfig()
		smuxConfig.MaxReceiveBuffer = config.SockBuf
		smuxConfig.KeepAliveInterval = time.Duration(config.KeepAliveMS) * time.Millisecond
		smuxConfig.KeepAliveTimeout = time.Duration(config.KeepAliveTimeout) * time.Millisecond
		smuxConfig.MaxFrameSize = config.MaxFrameSize
		smuxConfig.MaxStreamBuffer = config.StreamBuf
		smuxConfig.EnableStreamBuffer = config.StreamBufEn
		smuxConfig.BoostTimeout = time.Duration(config.BoostTimeout) * time.Millisecond

		createConn := func() (*smux.Session, *kcp.UDPSession, error) {
			kcpconn, err := dialKCP(block)
			if err != nil {
				return nil, nil, errors.Wrap(err, "createConn()")
			}
			kcpconn.SetStreamMode(true)
			kcpconn.SetWriteDelay(false)
			kcpconn.SetNoDelay(config.NoDelay, config.Interval, config.Resend, config.NoCongestion)
			kcpconn.SetWindowSize(config.SndWnd, config.RcvWnd)
			kcpconn.SetMtu(config.MTU)
			kcpconn.SetACKNoDelay(config.AckNodelay)

			if err := kcpconn.SetDSCP(config.DSCP); err != nil {
				log.Println("SetDSCP:", err)
			}
			if err := kcpconn.SetReadBuffer(config.SockBuf); err != nil {
				log.Println("SetReadBuffer:", err)
			}
			if err := kcpconn.SetWriteBuffer(config.SockBuf); err != nil {
				log.Println("SetWriteBuffer:", err)
			}

			// stream multiplex
			var session *smux.Session
			if config.NoComp {
				session, err = smux.Client(kcpconn, smuxConfig)
			} else {
				session, err = smux.Client(newCompStream(kcpconn), smuxConfig)
			}
			if err != nil {
				kcpconn.Close()
				return nil, nil, errors.Wrap(err, "createConn()")
			}
			log.Println("connection:", kcpconn.LocalAddr(), "->", kcpconn.RemoteAddr())
			return session, kcpconn, nil
		}

		// wait until a connection is ready
		waitConn := func() (*smux.Session, *kcp.UDPSession) {
			for {
				if session, kcpconn, err := createConn(); err == nil {
					return session, kcpconn
				} else {
					log.Println("re-connecting:", err)
					time.Sleep(time.Second)
				}
			}
		}
	go snmpLogger(config.SnmpLog, config.SnmpPeriod)

	session, kcpconn := waitConn()
	for {
		p1, err := listener.AcceptTCP()
		if err != nil {
			return err
		}
		if session.IsClosed() {
			// tear down the dead KCP session (closes its bus conn) and
			// dial a fresh one; dialKCP announces the new run so the
			// server rebuilds its listener for the new conv.
			kcpconn.Close()
			session, kcpconn = waitConn()
		}
		go handleClient(session, p1, config.Quiet, config.PipeBuf, config.Service)
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
