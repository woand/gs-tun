package kcpserver

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

func replyAndClose(p1 net.Conn, rpy int) {
	p1.Write([]byte{0x05, byte(rpy), 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	p1.Close()
}

func readSocksBackend(r io.Reader, h [4]byte) (string, error) {
	if h[0] != 0x05 || h[1] != 0x01 {
		return "", fmt.Errorf("unsupported SOCKS5 request")
	}
	var host string
	var portBytes [2]byte
	switch h[3] {
	case 0x01:
		var tail [6]byte
		if _, err := io.ReadFull(r, tail[:]); err != nil {
			return "", err
		}
		host = net.IPv4(tail[0], tail[1], tail[2], tail[3]).String()
		portBytes[0], portBytes[1] = tail[4], tail[5]
	case 0x03:
		var length [1]byte
		if _, err := io.ReadFull(r, length[:]); err != nil {
			return "", err
		}
		body := make([]byte, int(length[0])+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return "", err
		}
		host = string(body[:len(body)-2])
		portBytes[0], portBytes[1] = body[len(body)-2], body[len(body)-1]
	case 0x04:
		var tail [18]byte
		if _, err := io.ReadFull(r, tail[:]); err != nil {
			return "", err
		}
		host = net.IP(tail[:16]).String()
		portBytes[0], portBytes[1] = tail[16], tail[17]
	default:
		return "", fmt.Errorf("unsupported SOCKS5 address type %d", h[3])
	}
	port := int(portBytes[0])<<8 | int(portBytes[1])
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func handleFast(p1 net.Conn, quiet bool, buffersize int, tfo bool) {
	var h [4]byte
	if _, err := io.ReadFull(p1, h[:]); err != nil {
		Vlogln(quiet, "[fast client read]", p1, err)
		return
	}

	var backend string
	var err error
	if h[3] == 0x05 { // internal HTTP CONNECT framing: header + host:port
		var b [320]byte
		n, readErr := p1.Read(b[:])
		if readErr != nil {
			Vlogln(quiet, "[fast custom read]", p1, readErr)
			return
		}
		backend = string(b[:n])
	} else {
		backend, err = readSocksBackend(p1, h)
		if err != nil {
			replyAndClose(p1, 0x08)
			return
		}
	}

	var p2 net.Conn
	if tfo {
		p2, err = handleTFO(p1, backend, 5*time.Second)
	} else {
		p2, err = net.DialTimeout("tcp", backend, 5*time.Second)
	}
	if err != nil {
		Vlogln(quiet, "[err]", backend, err)

		switch t := err.(type) {
		case *net.AddrError:
			replyAndClose(p1, 0x03) // X'03' Network unreachable

		case *net.OpError:
			if t.Timeout() {
				replyAndClose(p1, 0x06) // X'06' TTL expired
			} else if t.Op == "dial" {
				replyAndClose(p1, 0x05) // X'05' Connection refused
			}

		default:
			//replyAndClose(p1, 0x03) // X'03' Network unreachable
			//replyAndClose(p1, 0x04) // X'04' Host unreachable
			replyAndClose(p1, 0x05) // X'05' Connection refused
			//replyAndClose(p1, 0x06) // X'06' TTL expired
		}
		return
	}
	defer p2.Close()

	Vlogln(quiet, "[got]", backend)
	reply := []byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	p1.Write(reply) // reply OK

	cp(p1, p2, buffersize)
	Vlogln(quiet, "[cls]", backend)
}

func handleClient(p1 net.Conn, quiet bool, buffersize int, serv string, target string, tfo bool) {
	if !quiet {
		log.Println("stream opened")
		defer log.Println("stream closed")
	}

	defer p1.Close()
	switch serv {
	default:
		fallthrough
	case "raw":

		var p2 net.Conn
		var err error
		if tfo {
			p2, err = handleTFO(p1, target, 5*time.Second)
		} else {
			p2, err = net.DialTimeout("tcp", target, 5*time.Second)
		}
		if err != nil {
			Vlogln(quiet, "[connect err]", target, err)
			return
		}
		defer p2.Close()
		cp(p1, p2, buffersize)

	case "fast":
		handleFast(p1, quiet, buffersize, tfo)
	}

}

func cp(p1 net.Conn, p2 net.Conn, buffersize int) {
	defer p2.Close()

	// start tunnel
	p1die := make(chan struct{})
	buf1 := make([]byte, buffersize)
	go func() { io.CopyBuffer(p1, p2, buf1); close(p1die) }()

	p2die := make(chan struct{})
	buf2 := make([]byte, buffersize)
	go func() { io.CopyBuffer(p2, p1, buf2); close(p2die) }()

	// wait for tunnel termination
	select {
	case <-p1die:
	case <-p2die:
	}
}

func Vlogln(quiet bool, v ...interface{}) {
	if !quiet {
		log.Println(v...)
	}
}
