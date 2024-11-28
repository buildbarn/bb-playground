package main

import (
	"crypto/ecdh"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"log"
	"math/big"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: create_x25519_client_certificate ca_certificate_path ca_private_key_path")
	}

	privateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		log.Fatal("Failed to generate X25519 private key: ", err)
	}
	marshaledPrivateKey, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		log.Fatal("Failed to marshal X25519 private key: ", err)
	}

	caCertificateData, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatal("Failed to read CA certificate: ", err)
	}
	caCertificateBlock, _ := pem.Decode(caCertificateData)
	if caCertificateBlock == nil {
		log.Fatal("CA certificate does not contain a PEM block")
	}
	if caCertificateBlock.Type != "CERTIFICATE" {
		log.Fatal("PEM block of CA private key is not of type CERTIFICATE")
	}
	caCertificate, err := x509.ParseCertificate(caCertificateBlock.Bytes)
	if err != nil {
		log.Fatal("Invalid CA certificate: ", err)
	}

	caPrivateKeyData, err := os.ReadFile(os.Args[2])
	if err != nil {
		log.Fatal("Failed to read CA private key: ", err)
	}
	caPrivateKeyBlock, _ := pem.Decode(caPrivateKeyData)
	if caPrivateKeyBlock == nil {
		log.Fatal("CA private key does not contain a PEM block")
	}
	if caPrivateKeyBlock.Type != "PRIVATE KEY" {
		log.Fatal("PEM block of CA private key is not of type PRIVATE KEY")
	}
	caPrivateKey, err := x509.ParsePKCS8PrivateKey(caPrivateKeyBlock.Bytes)
	if err != nil {
		log.Fatal("Invalid CA private key: ", err)
	}

	certificate, err := x509.CreateCertificate(
		rand.Reader,
		/* template = */ &x509.Certificate{
			SerialNumber: big.NewInt(1),
		},
		caCertificate,
		privateKey.Public(),
		caPrivateKey,
	)
	if err != nil {
		log.Fatal("Failed to create certificate: ", err)
	}

	pem.Encode(os.Stdout, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: marshaledPrivateKey,
	})
	pem.Encode(os.Stdout, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certificate,
	})
}
