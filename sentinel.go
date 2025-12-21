package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// runSentinelCheck connects to the DB using mTLS and sends a PING.
func runSentinelCheck(addr string, clientCert, clientKey, serverCA string) error {
	dialer := net.Dialer{Timeout: 2 * time.Second}
	var conn net.Conn
	var err error

	// Strict mTLS Config
	tlsConfig := &tls.Config{}

	// Load CA to verify server
	caCert, err := os.ReadFile(serverCA)
	if err != nil {
		return fmt.Errorf("failed to load sentinel CA: %w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	// Load Client Certs to authenticate to server
	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return fmt.Errorf("failed to load sentinel client certs: %w", err)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	conn, err = tls.DialWithDialer(&dialer, "tcp", addr, tlsConfig)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return fmt.Errorf("failed to set deadline: %w", err)
	}

	// 1. Ping (Authentication is handled by mTLS handshake)
	if err := sendPing(conn); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}

	return nil
}

func sendPing(conn net.Conn) error {
	// Ping has no payload
	reqBuf := make([]byte, 5)
	reqBuf[0] = OpCodePing
	binary.BigEndian.PutUint32(reqBuf[1:5], 0)

	if _, err := conn.Write(reqBuf); err != nil {
		return err
	}

	return expectResponse(conn, ResStatusOK, "PONG")
}

func expectResponse(conn net.Conn, expectedStatus byte, expectedBody string) error {
	headerBuf := make([]byte, 5)
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		return err
	}

	status := headerBuf[0]
	payloadLen := binary.BigEndian.Uint32(headerBuf[1:])

	// Consume body regardless of status to keep protocol clean
	body := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			return err
		}
	}

	if status != expectedStatus {
		return fmt.Errorf("unexpected status: 0x%x (Msg: %s)", status, string(body))
	}

	if expectedBody != "" {
		if string(body) != expectedBody {
			return fmt.Errorf("unexpected response body: got '%s', want '%s'", string(body), expectedBody)
		}
	}

	return nil
}
