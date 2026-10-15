package kcpclient

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/cs8425/smux"
)

func replyAndClose(p1 net.Conn, rpy int) {
	p1.Write([]byte{0x05, byte(rpy), 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	p1.Close()
}

func readSocksRequest(r io.Reader) ([]byte, string, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, "", err
	}
	if h[0] != 0x05 || h[1] != 0x01 {
		return nil, "", fmt.Errorf("unsupported SOCKS5 request")
	}
	request := append([]byte(nil), h[:]...)
	var host string
	switch h[3] {
	case 0x01:
		var tail [6]byte
		if _, err := io.ReadFull(r, tail[:]); err != nil {
			return nil, "", err
		}
		request = append(request, tail[:]...)
		host = net.IPv4(tail[0], tail[1], tail[2], tail[3]).String()
	case 0x03:
		var length [1]byte
		if _, err := io.ReadFull(r, length[:]); err != nil {
			return nil, "", err
		}
		body := make([]byte, int(length[0])+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, "", err
		}
		request = append(request, length[0])
		request = append(request, body...)
		host = string(body[:len(body)-2])
	case 0x04:
		var tail [18]byte
		if _, err := io.ReadFull(r, tail[:]); err != nil {
			return nil, "", err
		}
		request = append(request, tail[:]...)
		host = net.IP(tail[:16]).String()
	default:
		return nil, "", fmt.Errorf("unsupported SOCKS5 address type %d", h[3])
	}
	port := int(request[len(request)-2])<<8 | int(request[len(request)-1])
	return request, net.JoinHostPort(host, strconv.Itoa(port)), nil
}

// The local SOCKS reply is optimistic: application bytes can be pipelined
// while the remote side is still dialing. The remote SOCKS reply is consumed
// on the smux stream and never exposed to the local client.
func handleSocks(p1 net.Conn, sess *smux.Session, quiet bool, buffersize int) {
	var greeting [2]byte
	if _, err := io.ReadFull(p1, greeting[:]); err != nil {
		Vlogln(quiet, "socks client read", p1, err)
		return
	}
	if greeting[0] != 0x05 {
		return
	}
	methods := make([]byte, int(greeting[1]))
	if _, err := io.ReadFull(p1, methods); err != nil {
		return
	}
	if !bytes.Contains(methods, []byte{0x00}) {
		replyAndClose(p1, 0xff)
		return
	}
	p1.Write([]byte{0x05, 0x00})

	request, backend, err := readSocksRequest(p1)
	if err != nil {
		replyAndClose(p1, 0x07)
		return
	}
	p2, err := sess.OpenStream()
	if err != nil {
		return
	}
	defer p2.Close()
	if _, err := p2.Write(request); err != nil {
		return
	}

	if _, err := p1.Write([]byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); err != nil {
		return
	}
	remoteReady := make(chan error, 1)
	go func() {
		var remoteReply [10]byte
		_, err := io.ReadFull(p2, remoteReply[:])
		if err == nil && remoteReply[1] != 0x00 {
			err = fmt.Errorf("remote SOCKS reply %d", remoteReply[1])
		}
		remoteReady <- err
	}()
	clientDone := make(chan struct{})
	go func() { io.CopyBuffer(p2, p1, make([]byte, buffersize)); close(clientDone) }()
	select {
	case err := <-remoteReady:
		if err != nil {
			Vlogln(quiet, "Dial err:", backend, err)
			return
		}
	case <-clientDone:
		return
	}

	Vlogln(quiet, "socks to:", backend)
	remoteDone := make(chan struct{})
	go func() { io.CopyBuffer(p1, p2, make([]byte, buffersize)); close(remoteDone) }()
	select {
	case <-clientDone:
	case <-remoteDone:
	}
}

// thanks: http://www.golangnote.com/topic/141.html
func handleHttp(client net.Conn, sess *smux.Session, quiet bool, buffersize int) {
	var b [1024]byte
	n, err := client.Read(b[:])
	if err != nil {
		Vlogln(quiet, "http client read err", client, err)
		return
	}
	var method, host, address string
	idx := bytes.IndexByte(b[:], '\n')
	if idx == -1 {
		Vlogln(quiet, "http client parse err", idx, client.RemoteAddr())
		return
	}
	fmt.Sscanf(string(b[:idx]), "%s%s", &method, &host)

	if strings.Index(host, "://") == -1 {
		host = "//" + host
	}
	hostPortURL, err := url.Parse(host)
	if err != nil {
		Vlogln(quiet, "Parse hostPortURL err:", client, hostPortURL, err)
		return
	}
	if strings.Index(hostPortURL.Host, ":") == -1 { // no port, default 80
		address = hostPortURL.Host + ":80"
	} else {
		address = hostPortURL.Host
	}

	p2, err := sess.OpenStream()
	if err != nil {
		return
	}
	defer p2.Close()

	Vlogln(quiet, "Dial to:", method, address)
	var target = append([]byte{0, 0, 0, 0x05}, []byte(address)...)
	p2.Write(target)

	var b2 [10]byte
	n2, err := io.ReadFull(p2, b2[:])
	if err != nil {
		Vlogln(quiet, "Dial err replay:", address, n2, err)
		return
	}
	if err != nil || b2[1] != 0x00 {
		Vlogln(quiet, "Dial err:", address, n2, b2[1], err)
		return
	}

	if method == "CONNECT" {
		client.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
	} else {
		p2.Write(b[:n])
	}

	cp(client, p2, buffersize)
}

func handleClient(sess *smux.Session, p1 net.Conn, quiet bool, buffersize int, serv string) {
	if !quiet {
		log.Println("stream opened")
		defer log.Println("stream closed")
	}

	defer p1.Close()
	switch serv {
	case "socks5":
		handleSocks(p1, sess, quiet, buffersize)

	case "http":
		handleHttp(p1, sess, quiet, buffersize)

	default:
		p2, err := sess.OpenStream()
		if err != nil {
			return
		}
		defer p2.Close()
		cp(p1, p2, buffersize)
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
