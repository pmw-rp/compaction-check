package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// buildTLSConfig builds a *tls.Config for the Kafka connection. caFile adds
// a CA to trust alongside the system roots; certFile/keyFile enable mTLS;
// insecureSkipVerify disables server certificate verification entirely
// (test/dev use only - never for a real cluster).
func buildTLSConfig(caFile, certFile, keyFile string, insecureSkipVerify bool) (*tls.Config, error) {
	cfg := &tls.Config{InsecureSkipVerify: insecureSkipVerify}

	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read -tls-ca %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in -tls-ca %s", caFile)
		}
		cfg.RootCAs = pool
	}

	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("-tls-cert and -tls-key must both be set for mTLS")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load -tls-cert/-tls-key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

// buildSASLMechanism builds the SASL mechanism named by -sasl-mechanism.
func buildSASLMechanism(mechanism, user, pass string) (sasl.Mechanism, error) {
	if user == "" || pass == "" {
		return nil, fmt.Errorf("-sasl-user and -sasl-pass are required with -sasl-mechanism")
	}
	switch strings.ToUpper(mechanism) {
	case "PLAIN":
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported -sasl-mechanism %q (want PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)", mechanism)
	}
}
