package analysis

import (
	"context"
	"crypto/ecdh"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"log"

	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	remoteexecution_pb "github.com/buildbarn/bb-playground/pkg/proto/remoteexecution"
	"github.com/secure-io/siv-go"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func (c *baseComputer) ComputeActionResultValue(ctx context.Context, key model_core.Message[*model_analysis_pb.ActionResult_Key], e ActionResultEnvironment) (PatchedActionResultValue, error) {
	// Compute shared secret for encrypting the action.
	platformPublicKey, err := x509.ParsePKIXPublicKey(key.Message.PlatformPkixPublicKey)
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("invalid platform PKIX public key: %w", err)
	}
	platformECDHPublicKey, ok := platformPublicKey.(*ecdh.PublicKey)
	if !ok {
		return PatchedActionResultValue{}, errors.New("platform PKIX public key is not an ECDH public key")
	}
	sharedSecret, err := c.executionClientPrivateKey.ECDH(platformECDHPublicKey)
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("failed to obtain shared secret: %w", err)
	}

	// Use the reference of the Command message as the stable
	// fingerprint of the action, which the scheduler can use to
	// keep track of performance characteristics. Compute a hash to
	// masquerade the actual Command reference.
	commandReferenceIndex, err := model_core.GetIndexFromReferenceMessage(key.Message.CommandReference, key.OutgoingReferences.GetDegree())
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("invalid command reference: %w", err)
	}
	commandReferenceSHA256 := sha256.Sum256(key.OutgoingReferences.GetOutgoingReference(commandReferenceIndex).GetRawReference())

	actionAdditionalData := &remoteexecution_pb.Action_AdditionalData{
		StableFingerprint: commandReferenceSHA256[:],
		ExecutionTimeout:  key.Message.ExecutionTimeout,
	}
	marshaledActionAdditionalData, err := proto.Marshal(actionAdditionalData)
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("failed to marshal action additional data: %w", err)
	}

	actionKey := append([]byte(nil), sharedSecret...)
	actionKey[0] ^= 1
	actionAEAD, err := siv.NewGCM(actionKey)
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("failed to create AES-GCM-SIV for action: %w", err)
	}
	actionNonce := make([]byte, actionAEAD.NonceSize())

	client, err := c.executionClient.Execute(ctx, &remoteexecution_pb.ExecuteRequest{
		Action: &remoteexecution_pb.Action{
			PlatformPkixPublicKey:  key.Message.PlatformPkixPublicKey,
			ClientCertificateChain: c.executionClientCertificateChain,
			Nonce:                  actionNonce,
			AdditionalData:         actionAdditionalData,
			// TODO: Provide a proper action message!
			Ciphertext: actionAEAD.Seal(nil, actionNonce, []byte("Hello"), marshaledActionAdditionalData),
		},
		// TODO: Priority.
	})
	if err != nil {
		return PatchedActionResultValue{}, err
	}
	for {
		response, err := client.Recv()
		if err != nil {
			return PatchedActionResultValue{}, err
		}
		log.Print(protojson.Format(response))
	}
}
