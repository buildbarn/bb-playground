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

	"github.com/buildbarn/bonanza/pkg/evaluation"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/model/core/btree"
	model_encoding "github.com/buildbarn/bonanza/pkg/model/encoding"
	model_parser "github.com/buildbarn/bonanza/pkg/model/parser"
	model_analysis_pb "github.com/buildbarn/bonanza/pkg/proto/model/analysis"
	model_command_pb "github.com/buildbarn/bonanza/pkg/proto/model/command"
	remoteexecution_pb "github.com/buildbarn/bonanza/pkg/proto/remoteexecution"
	"github.com/buildbarn/bonanza/pkg/storage/dag"
	"github.com/buildbarn/bonanza/pkg/storage/object"

	"google.golang.org/grpc/status"
)

func (c *baseComputer) ComputeActionResultValue(ctx context.Context, key model_core.Message[*model_analysis_pb.ActionResult_Key, object.OutgoingReferences[object.LocalReference]], e ActionResultEnvironment) (PatchedActionResultValue, error) {
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

	// Use the reference of the Command message as the stable
	// fingerprint of the action, which the scheduler can use to
	// keep track of performance characteristics. Compute a hash to
	// masquerade the actual Command reference.
	commandReference, err := model_core.FlattenReference(model_core.NewNestedMessage(key, key.Message.CommandReference))
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("invalid command reference: %w", err)
	}
	commandReferenceSHA256 := sha256.Sum256(commandReference.GetRawReference())

	inputRootReference, err := model_core.FlattenReference(model_core.NewNestedMessage(key, key.Message.InputRootReference))
	if err != nil {
		return PatchedActionResultValue{}, fmt.Errorf("invalid input root reference: %w", err)
	}

	namespace := c.buildSpecificationReference.GetNamespace()
	var completionEvent model_command_pb.Result
	var errExecution error
	for range c.executionClient.RunAction(
		ctx,
		platformECDHPublicKey,
		&model_command_pb.Action{
			Namespace:          namespace.ToProto(),
			CommandEncoders:    commandEncodersValue.Message.CommandEncoders,
			CommandReference:   commandReference.GetRawReference(),
			InputRootReference: inputRootReference.GetRawReference(),
		},
		&remoteexecution_pb.Action_AdditionalData{
			StableFingerprint: commandReferenceSHA256[:],
			ExecutionTimeout:  key.Message.ExecutionTimeout,
		},
		&completionEvent,
		&errExecution,
	) {
		// TODO: Capture and propagate execution events?
	}
	if errExecution != nil {
		return PatchedActionResultValue{}, errExecution
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

func (c *baseComputer) convertDictToEnvironmentVariableList(environment map[string]string, commandEncoder model_encoding.BinaryEncoder) (model_core.PatchedMessage[[]*model_command_pb.EnvironmentVariableList_Element, dag.ObjectContentsWalker], error) {
	environmentVariablesBuilder := btree.NewSplitProllyBuilder(
		1<<16,
		1<<18,
		btree.NewObjectCreatingNodeMerger(
			commandEncoder,
			c.getReferenceFormat(),
			/* parentNodeComputer = */ func(createdObject model_core.CreatedObject[dag.ObjectContentsWalker], childNodes []*model_command_pb.EnvironmentVariableList_Element) (model_core.PatchedMessage[*model_command_pb.EnvironmentVariableList_Element, dag.ObjectContentsWalker], error) {
				patcher := model_core.NewReferenceMessagePatcher[dag.ObjectContentsWalker]()
				return model_core.NewPatchedMessage(
					&model_command_pb.EnvironmentVariableList_Element{
						Level: &model_command_pb.EnvironmentVariableList_Element_Parent{
							Parent: patcher.AddReference(
								createdObject.Contents.GetReference(),
								dag.NewSimpleObjectContentsWalker(createdObject.Contents, createdObject.Metadata),
							),
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

func (c *baseComputer) getOutputsFromActionResult(ctx context.Context, actionResult model_core.Message[*model_analysis_pb.ActionResult_Value, object.OutgoingReferences[object.LocalReference]], directoryReaders *DirectoryReaders) (model_core.Message[*model_command_pb.Outputs, object.OutgoingReferences[object.LocalReference]], error) {
	if actionResult.Message.OutputsReference == nil {
		// Action did not yield any outputs. Return an empty
		// outputs message, so any code that attempts to access
		// individual outputs behaves well.
		return model_core.NewMessage(&model_command_pb.Outputs{}, object.OutgoingReferences[object.LocalReference](object.OutgoingReferencesList{})), nil
	}

	return model_parser.Dereference(ctx, directoryReaders.CommandOutputs, model_core.NewNestedMessage(actionResult, actionResult.Message.OutputsReference))
}
