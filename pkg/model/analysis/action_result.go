package analysis

import (
	"context"
	"crypto/ecdh"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/buildbarn/bb-playground/pkg/evaluation"
	model_core "github.com/buildbarn/bb-playground/pkg/model/core"
	"github.com/buildbarn/bb-playground/pkg/model/core/btree"
	model_encoding "github.com/buildbarn/bb-playground/pkg/model/encoding"
	model_filesystem "github.com/buildbarn/bb-playground/pkg/model/filesystem"
	model_parser "github.com/buildbarn/bb-playground/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bb-playground/pkg/proto/model/analysis"
	model_command_pb "github.com/buildbarn/bb-playground/pkg/proto/model/command"
	remoteexecution_pb "github.com/buildbarn/bb-playground/pkg/proto/remoteexecution"
	"github.com/buildbarn/bb-playground/pkg/storage/dag"
	"github.com/buildbarn/bb-playground/pkg/storage/object"
	"github.com/secure-io/siv-go"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func (c *baseComputer) ComputeActionResultValue(ctx context.Context, key model_core.Message[*model_analysis_pb.ActionResult_Key], e ActionResultEnvironment) (PatchedActionResultValue, error) {
	commandEncodersValue := e.GetCommandEncodersValue(&model_analysis_pb.CommandEncoders_Key{})
	if !commandEncodersValue.IsSet() {
		return PatchedActionResultValue{}, evaluation.ErrMissingDependency
	}

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
	commandReference := key.OutgoingReferences.GetOutgoingReference(commandReferenceIndex).GetRawReference()
	commandReferenceSHA256 := sha256.Sum256(commandReference)

	inputRootReferenceIndex, err := model_core.GetIndexFromReferenceMessage(key.Message.InputRootReference, key.OutgoingReferences.GetDegree())
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("invalid input root reference: %w", err)
	}

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

	namespace := c.buildSpecificationReference.GetNamespace()
	commandAction := model_command_pb.Action{
		Namespace:          namespace.ToProto(),
		CommandEncoders:    commandEncodersValue.Message.CommandEncoders,
		CommandReference:   commandReference,
		InputRootReference: key.OutgoingReferences.GetOutgoingReference(inputRootReferenceIndex).GetRawReference(),
	}
	commandActionAny, err := anypb.New(&commandAction)
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("failed to embed action into Any message: %w", err)
	}
	commandActionData, err := proto.Marshal(commandActionAny)
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("failed to marshal action: %w", err)
	}
	actionCiphertext := actionAEAD.Seal(nil, actionNonce, commandActionData, marshaledActionAdditionalData)

	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	client, err := c.executionClient.Execute(ctxWithCancel, &remoteexecution_pb.ExecuteRequest{
		Action: &remoteexecution_pb.Action{
			PlatformPkixPublicKey:  key.Message.PlatformPkixPublicKey,
			ClientCertificateChain: c.executionClientCertificateChain,
			Nonce:                  actionNonce,
			AdditionalData:         actionAdditionalData,
			Ciphertext:             actionCiphertext,
		},
		// TODO: Priority.
	})
	if err != nil {
		return PatchedActionResultValue{}, err
	}
	defer func() {
		cancel()
		for {
			if _, err := client.Recv(); err != nil {
				return
			}
		}
	}()

	executionEventAdditionalData := sha256.Sum256(actionCiphertext)
	for {
		response, err := client.Recv()
		if err != nil {
			return PatchedActionResultValue{}, err
		}

		if completed, ok := response.Stage.(*remoteexecution_pb.ExecuteResponse_Completed_); ok {
			completionEventMessage := completed.Completed.CompletionEvent
			if completionEventMessage == nil {
				return PatchedActionResultValue{}, errors.New("action completed, but no completion event was returned")
			}

			completionEventKey := append([]byte(nil), sharedSecret...)
			completionEventKey[0] ^= 3
			completionEventAEAD, err := siv.NewGCM(completionEventKey)
			if err != nil {
				return PatchedActionResultValue{}, fmt.Errorf("failed to create AES-GCM-SIV for completion event: %w", err)
			}

			completionEventData, err := completionEventAEAD.Open(nil, completionEventMessage.Nonce, completionEventMessage.Ciphertext, executionEventAdditionalData[:])
			if err != nil {
				return PatchedActionResultValue{}, fmt.Errorf("failed to decrypt completion event: %w", err)
			}

			var completionEvent model_command_pb.Result
			if err := proto.Unmarshal(completionEventData, &completionEvent); err != nil {
				return PatchedActionResultValue{}, fmt.Errorf("failed to unmarshal completion event: %w", err)
			}

			if err := status.ErrorProto(completionEvent.Status); err != nil {
				return PatchedActionResultValue{}, err
			}
			if key.Message.ExitCodeMustBeZero && completionEvent.ExitCode != 0 {
				return PatchedActionResultValue{}, fmt.Errorf("action completed with non-zero exit code %d", completionEvent.ExitCode)
			}

			result := &model_analysis_pb.ActionResult_Value{
				ExitCode: completionEvent.ExitCode,
			}
			patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
			if len(completionEvent.OutputsReference) > 0 {
				outputsReference, err := namespace.NewLocalReference(completionEvent.OutputsReference)
				if err != nil {
					return PatchedActionResultValue{}, fmt.Errorf("invalid outputs reference: %w", err)
				}
				result.OutputsReference = patcher.AddReference(outputsReference, dag.ExistingObjectContentsWalker)
			}
			return model_core.NewPatchedMessage(result, patcher), nil
		}
	}
}

func (c *baseComputer) convertDictToEnvironmentVariableList(environment map[string]string, commandEncoder model_encoding.BinaryEncoder) (model_core.PatchedMessage[[]*model_command_pb.EnvironmentVariableList_Element, dag.ObjectContentsWalker], error) {
	environmentVariablesBuilder := btree.NewSplitProllyBuilder(
		1<<16,
		1<<18,
		btree.NewObjectCreatingNodeMerger(
			commandEncoder,
			c.buildSpecificationReference.GetReferenceFormat(),
			/* parentNodeComputer = */ func(contents *object.Contents, childNodes []*model_command_pb.EnvironmentVariableList_Element, outgoingReferences object.OutgoingReferences, metadata []dag.ObjectContentsWalker) (model_core.PatchedMessage[*model_command_pb.EnvironmentVariableList_Element, dag.ObjectContentsWalker], error) {
				patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
				return model_core.NewPatchedMessage(
					&model_command_pb.EnvironmentVariableList_Element{
						Level: &model_command_pb.EnvironmentVariableList_Element_Parent{
							Parent: patcher.AddReference(contents.GetReference(), dag.NewSimpleObjectContentsWalker(contents, metadata)),
						},
					},
					patcher,
				), nil
			},
		),
	)
	for _, name := range slices.Sorted(maps.Keys(environment)) {
		if err := environmentVariablesBuilder.PushChild(
			model_core.NewSimplePatchedMessage[dag.ObjectContentsWalker](&model_command_pb.EnvironmentVariableList_Element{
				Level: &model_command_pb.EnvironmentVariableList_Element_Leaf_{
					Leaf: &model_command_pb.EnvironmentVariableList_Element_Leaf{
						Name:  name,
						Value: environment[name],
					},
				},
			}),
		); err != nil {
			return model_core.PatchedMessage[[]*model_command_pb.EnvironmentVariableList_Element, dag.ObjectContentsWalker]{}, err
		}
	}
	return environmentVariablesBuilder.FinalizeList()
}

func (c *baseComputer) getOutputsFromActionResult(ctx context.Context, actionResult model_core.Message[*model_analysis_pb.ActionResult_Value], directoryAccessParameters *model_filesystem.DirectoryAccessParameters) (model_core.Message[*model_command_pb.Outputs], error) {
	if actionResult.Message.OutputsReference == nil {
		// Action did not yield any outputs. Return an empty
		// outputs message, so any code that attempts to access
		// individual outputs behaves well.
		return model_core.NewSimpleMessage(&model_command_pb.Outputs{}), nil
	}

	outputsReader := model_parser.NewStorageBackedParsedObjectReader(
		c.objectDownloader,
		directoryAccessParameters.GetEncoder(),
		model_parser.NewMessageObjectParser[object.LocalReference, model_command_pb.Outputs](),
	)
	outputsReferenceIndex, err := model_core.GetIndexFromReferenceMessage(actionResult.Message.OutputsReference, actionResult.OutgoingReferences.GetDegree())
	if err != nil {
		return model_core.Message[*model_command_pb.Outputs]{}, fmt.Errorf("invalid command outputs reference index: %w", err)
	}
	outputs, _, err := outputsReader.ReadParsedObject(ctx, actionResult.OutgoingReferences.GetOutgoingReference(outputsReferenceIndex))
	return outputs, err
}
