package remoteworker

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/ecdh"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"io"
	"log"
	"sort"
	"sync/atomic"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/otel"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bonanza/pkg/ds"
	remoteexecution_pb "github.com/buildbarn/bonanza/pkg/proto/remoteexecution"
	remoteworker_pb "github.com/buildbarn/bonanza/pkg/proto/remoteworker"
	"github.com/secure-io/siv-go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client for the Remote Worker protocol. It can send synchronization
// requests to a scheduler, informing it of the current state of the
// worker, while also obtaining requests for executing actions.
type Client[
	TAction any,
	TActionPtr interface {
		*TAction
		proto.Message
	},
] struct {
	// Constant fields.
	scheduler                    remoteworker_pb.OperationQueueClient
	executor                     Executor[*TAction]
	clock                        clock.Clock
	randomNumberGenerator        random.ThreadSafeGenerator
	privateKeys                  []*ecdh.PrivateKey
	clientCertificateAuthorities *x509.CertPool
	isLargestSizeClass           bool

	// Mutable fields that are always set.
	request                         remoteworker_pb.SynchronizeRequest
	schedulerMayThinkExecutingUntil *time.Time
	nextSynchronizationAt           time.Time

	// Mutable fields that are only set when executing an action.
	executionCancellation              func()
	executionCompleted                 <-chan struct{}
	latestExecutionEvent               atomic.Pointer[proto.Message]
	completionEvent                    proto.Message
	completionVirtualExecutionDuration time.Duration
	completionResult                   remoteworker_pb.CurrentState_Completed_Result
	executionSharedSecret              []byte
	eventAdditionalData                [sha256.Size]byte
}

type clientKey struct {
	privateKey    *ecdh.PrivateKey
	pkixPublicKey []byte
}

type clientKeyList struct {
	ds.Slice[clientKey]
}

func (l clientKeyList) Less(i, j int) bool {
	return bytes.Compare(l.Slice[i].pkixPublicKey, l.Slice[j].pkixPublicKey) < 0
}

// NewClient creates a new Client instance that is set to the initial
// state (i.e., being idle).
func NewClient[
	TAction any,
	TActionPtr interface {
		*TAction
		proto.Message
	},
](
	scheduler remoteworker_pb.OperationQueueClient,
	executor Executor[*TAction],
	clock clock.Clock,
	randomNumberGenerator random.ThreadSafeGenerator,
	unsortedPrivateKeys []*ecdh.PrivateKey,
	clientCertificateAuthorities *x509.CertPool,
	workerID map[string]string,
	sizeClass uint32,
	isLargestSizeClass bool,
) (*Client[TAction, TActionPtr], error) {
	// Public keys need to be sent to the scheduler in sorted order.
	// Also sort our private keys the same way, so that we can
	// easily look up the private key corresponding to a public key
	// in an action.
	keysToSort := clientKeyList{
		Slice: make(ds.Slice[clientKey], 0, len(unsortedPrivateKeys)),
	}
	for i, privateKey := range unsortedPrivateKeys {
		pkixPublicKey, err := x509.MarshalPKIXPublicKey(privateKey.PublicKey())
		if err != nil {
			return nil, util.StatusWrapfWithCode(err, codes.InvalidArgument, "Invalid private key at index %d", i)
		}
		keysToSort.Push(clientKey{
			privateKey:    privateKey,
			pkixPublicKey: pkixPublicKey,
		})
	}
	sort.Sort(keysToSort)

	sortedPrivateKeys := make([]*ecdh.PrivateKey, 0, len(keysToSort.Slice))
	publicKeys := make([]*remoteworker_pb.SynchronizeRequest_PublicKey, 0, len(keysToSort.Slice))
	for _, key := range keysToSort.Slice {
		sortedPrivateKeys = append(sortedPrivateKeys, key.privateKey)
		publicKeys = append(publicKeys, &remoteworker_pb.SynchronizeRequest_PublicKey{
			PkixPublicKey: key.pkixPublicKey,
		})
	}

	return &Client[TAction, TActionPtr]{
		scheduler:                    scheduler,
		executor:                     executor,
		clock:                        clock,
		randomNumberGenerator:        randomNumberGenerator,
		privateKeys:                  sortedPrivateKeys,
		clientCertificateAuthorities: clientCertificateAuthorities,
		isLargestSizeClass:           isLargestSizeClass,

		request: remoteworker_pb.SynchronizeRequest{
			WorkerId:   workerID,
			PublicKeys: publicKeys,
			SizeClass:  sizeClass,
			CurrentState: &remoteworker_pb.CurrentState{
				WorkerState: &remoteworker_pb.CurrentState_Idle{
					Idle: &emptypb.Empty{},
				},
			},
		},
		nextSynchronizationAt: clock.Now(),
	}, nil
}

func (c *Client[TAction, TActionPtr]) startExecution(desiredState *remoteworker_pb.DesiredState_Executing) error {
	c.stopExecution()

	action := desiredState.Action
	if action == nil {
		return status.Error(codes.InvalidArgument, "No action provided")
	}

	// Determine which worker key is used.
	workerKeyIndex, ok := sort.Find(len(c.request.PublicKeys), func(i int) int {
		return bytes.Compare(action.PlatformPkixPublicKey, c.request.PublicKeys[i].PkixPublicKey)
	})
	if !ok {
		return status.Errorf(codes.InvalidArgument, "Worker received action for platform with PKIX public key %s, even though it does not announce this key", base64.StdEncoding.EncodeToString(action.PlatformPkixPublicKey))
	}

	// Validate the provided client certificate.
	var clientCertificate *x509.Certificate
	intermediates := x509.NewCertPool()
	for i, rawCertificate := range action.ClientCertificateChain {
		certificate, err := x509.ParseCertificate(rawCertificate)
		if err != nil {
			return util.StatusWrapfWithCode(err, codes.InvalidArgument, "Invalid certificate at index %d of client certificate chain", i)
		}
		if clientCertificate == nil {
			clientCertificate = certificate
		} else {
			intermediates.AddCert(certificate)
		}
	}
	if clientCertificate == nil {
		return status.Error(codes.InvalidArgument, "Empty client certificate chain")
	}
	if _, err := clientCertificate.Verify(x509.VerifyOptions{
		Intermediates: intermediates,
		Roots:         c.clientCertificateAuthorities,
		CurrentTime:   c.clock.Now(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to validate client certificate")
	}

	// Obtain the shared secret used to decrypt actions and encrypt
	// execution events.
	clientPublicKey, ok := clientCertificate.PublicKey.(*ecdh.PublicKey)
	if !ok {
		return status.Error(codes.InvalidArgument, "Client certificate does not contain an ECDH public key")
	}
	sharedSecret, err := c.privateKeys[workerKeyIndex].ECDH(clientPublicKey)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to compute shared secret")
	}

	// Decrypt and unmarshal the action payload.
	actionKey := append([]byte(nil), sharedSecret...)
	actionKey[0] ^= 1
	actionAEAD, err := siv.NewGCM(actionKey)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to create AEAD for decrypting action")
	}
	additionalData := action.AdditionalData
	if additionalData == nil {
		return status.Error(codes.InvalidArgument, "Action does not contain additional data")
	}
	marshaledAdditionalData, err := proto.Marshal(additionalData)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to marshal additional data")
	}
	plaintext, err := actionAEAD.Open(nil, action.Nonce, action.Ciphertext, marshaledAdditionalData)
	if err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to decrypting action")
	}
	var plaintextAny anypb.Any
	if err := proto.Unmarshal(plaintext, &plaintextAny); err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to unmarshal action")
	}
	var plaintextAction TAction
	if err := plaintextAny.UnmarshalTo(TActionPtr(&plaintextAction)); err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to unmarshal action")
	}

	// Validate that the execution timeout provided by the scheduler.
	if err := additionalData.ExecutionTimeout.CheckValid(); err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid original execution timeout")
	}
	originalExecutionTimeout := additionalData.ExecutionTimeout.AsDuration()
	if err := desiredState.EffectiveExecutionTimeout.CheckValid(); err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid effective execution timeout")
	}
	effectiveExecutionTimeout := desiredState.EffectiveExecutionTimeout.AsDuration()
	if effectiveExecutionTimeout < 0 || effectiveExecutionTimeout > originalExecutionTimeout {
		return status.Errorf(
			codes.InvalidArgument,
			"Execution timeout of %s is outside permitted range [0s, %s]",
			effectiveExecutionTimeout,
			originalExecutionTimeout,
		)
	}

	// Launch one goroutine that captures any execution events
	// generated by the executor.
	executionEvents := make(chan proto.Message, 1)
	storedAllExecutionEvents := make(chan struct{})
	go func() {
		for executionEvent := range executionEvents {
			c.latestExecutionEvent.Store(&executionEvent)
		}
		close(storedAllExecutionEvents)
	}()

	// Launch another goroutine that invokes the executor.
	ctx, executionCancellation := context.WithCancel(
		otel.NewContextWithW3CTraceContext(
			context.Background(),
			desiredState.W3CTraceContext))
	executionCompleted := make(chan struct{})
	go func() {
		event, virtualExecutionDuration, result := c.executor.Execute(ctx, &plaintextAction, effectiveExecutionTimeout, executionEvents)

		// Wait for the goroutine above to terminate, so that we
		// can guarantee that c.latestExecutionEvent is no
		// longer updated.
		close(executionEvents)
		<-storedAllExecutionEvents
		c.latestExecutionEvent.Store(nil)

		// Discard the completion event if execution failed and
		// the worker doesn't belong to the largest size class.
		// The scheduler MUST retry execution on the largest
		// size class.
		switch result {
		case remoteworker_pb.CurrentState_Completed_SUCCEEDED:
		case remoteworker_pb.CurrentState_Completed_TIMED_OUT:
			if !c.isLargestSizeClass || effectiveExecutionTimeout < originalExecutionTimeout {
				event = nil
			}
		case remoteworker_pb.CurrentState_Completed_FAILED:
			if !c.isLargestSizeClass {
				event = nil
			}
		default:
			panic("executor returned an invalid result")
		}

		// Allow Run() to embed the completion event into the
		// Synchronize() request.
		c.completionEvent = event
		c.completionVirtualExecutionDuration = virtualExecutionDuration
		c.completionResult = result
		close(executionCompleted)
	}()

	c.executionCancellation = executionCancellation
	c.executionCompleted = executionCompleted
	c.executionSharedSecret = sharedSecret
	c.eventAdditionalData = sha256.Sum256(action.Ciphertext)

	// Change state to indicate the build has started.
	c.request.CurrentState.WorkerState = &remoteworker_pb.CurrentState_Executing_{
		Executing: &remoteworker_pb.CurrentState_Executing{
			TaskUuid: desiredState.TaskUuid,
		},
	}
	return nil
}

func (c *Client[TAction, TActionPtr]) stopExecution() {
	// Trigger cancellation of the existing build action and wait
	// for it to complete. Discard the results.
	if c.executionCancellation != nil {
		c.executionCancellation()
		<-c.executionCompleted
		c.executionCancellation = nil
	}

	c.request.CurrentState.WorkerState = &remoteworker_pb.CurrentState_Idle{
		Idle: &emptypb.Empty{},
	}
}

// touchSchedulerMayThinkExecuting updates state on whether the
// scheduler may think the worker is currently executing an action. This
// is used to determine whether it is safe to terminate the worker
// gracefully.
func (c *Client[TAction, TActionPtr]) touchSchedulerMayThinkExecuting() {
	// Assume that if we've missed the desired synchronization time
	// provided by the scheduler by more than a minute, the
	// scheduler has purged our state.
	until := c.nextSynchronizationAt.Add(time.Minute)
	c.schedulerMayThinkExecutingUntil = &until
}

// marshalAndEncryptEvent marshals and encrypts an execution or
// completion event, so that it can be sent back to the scheduler in
// such a way that only the client is able to decrypt it.
func (c *Client[TAction, TActionPtr]) marshalAndEncryptEvent(event proto.Message, sharedSecretModifier byte) (*remoteexecution_pb.ExecutionEvent, error) {
	eventKey := append([]byte(nil), c.executionSharedSecret...)
	eventKey[0] ^= sharedSecretModifier
	eventAEAD, err := siv.NewGCM(eventKey)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to create AEAD for encrypting event")
	}
	nonce := make([]byte, eventAEAD.NonceSize())
	if _, err := io.ReadFull(c.randomNumberGenerator, nonce); err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to generate nonce")
	}
	marshaledEvent, err := proto.Marshal(event)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to marshal event")
	}
	return &remoteexecution_pb.ExecutionEvent{
		Nonce:      nonce,
		Ciphertext: eventAEAD.Seal(nil, nonce, marshaledEvent, c.eventAdditionalData[:]),
	}, nil
}

// Run an iteration of the Remote Worker client, by performing a single
// synchronization against the scheduler.
func (c *Client[TAction, TActionPtr]) Run(ctx context.Context) (bool, error) {
	// Allow the worker to terminate if the scheduler doesn't think
	// we're executing any action, or if we haven't been able to
	// successfully synchronize for a prolonged amount of time.
	if ctx.Err() != nil && (c.schedulerMayThinkExecutingUntil == nil || c.clock.Now().After(*c.schedulerMayThinkExecutingUntil)) {
		return true, nil
	}

	// If the scheduler isn't assuming we're executing any action
	// right now, perform some readiness checks. This ensures we
	// don't dequeue actions from the scheduler while unhealthy.
	if c.schedulerMayThinkExecutingUntil == nil {
		if err := c.executor.CheckReadiness(ctx); err != nil {
			return true, util.StatusWrap(err, "Worker failed readiness check")
		}
	}

	if workerState, ok := c.request.CurrentState.WorkerState.(*remoteworker_pb.CurrentState_Executing_); ok {
		// Either wait until we've reached the next
		// synchronization time or execution has completed.
		timer, timerChannel := c.clock.NewTimer(c.nextSynchronizationAt.Sub(c.clock.Now()))
		select {
		case <-timerChannel:
		case <-c.executionCompleted:
			timer.Stop()
		}

		select {
		case <-c.executionCompleted:
			// Action is completed. Transition the current state
			// from executing to completed.
			//
			// TODO: We should omit the event if we're not the
			// largest size class!
			var event *remoteexecution_pb.ExecutionEvent
			if c.completionEvent != nil {
				var err error
				event, err = c.marshalAndEncryptEvent(c.completionEvent, 3)
				if err != nil {
					return false, util.StatusWrap(err, "Failed to marshal completion event")
				}
			}
			c.request.CurrentState.WorkerState = &remoteworker_pb.CurrentState_Completed_{
				Completed: &remoteworker_pb.CurrentState_Completed{
					TaskUuid:                 workerState.Executing.TaskUuid,
					Event:                    event,
					VirtualExecutionDuration: durationpb.New(c.completionVirtualExecutionDuration),
					Result:                   c.completionResult,
				},
			}
			c.completionEvent = nil
		default:
			// Action is still executing. See if there are
			// any execution events that we can send to the
			// scheduler.
			if latestExecutionEvent := c.latestExecutionEvent.Swap(nil); latestExecutionEvent != nil {
				event, err := c.marshalAndEncryptEvent(*latestExecutionEvent, 2)
				if err != nil {
					return false, util.StatusWrap(err, "Failed to marshal execution event")
				}
				workerState.Executing.Event = event
			}
		}
	}

	// Determine whether we should perform call to Synchronize with
	// prefer_being_able set to false (potentially blocking) or true
	// (non-blocking).
	currentStateIsExecuting := false
	switch workerState := c.request.CurrentState.WorkerState.(type) {
	case *remoteworker_pb.CurrentState_Idle:
		// Even though we are idle, the scheduler may think we
		// are executing. This means we were not able to perform
		// readiness checks. Forcefully switch to idle, so that
		// we can still do this before picking up more work.
		c.request.PreferBeingIdle = c.schedulerMayThinkExecutingUntil != nil
	case *remoteworker_pb.CurrentState_Rejected_:
		c.request.PreferBeingIdle = false
	case *remoteworker_pb.CurrentState_Executing_:
		currentStateIsExecuting = true
		c.request.PreferBeingIdle = false
	case *remoteworker_pb.CurrentState_Completed_:
		// In case execution failed with a serious
		// error, request that the worker gets a brief
		// amount of idle time, so that we can do some
		// health checks prior to picking up more work.
		c.request.PreferBeingIdle = workerState.Completed.Result != remoteworker_pb.CurrentState_Completed_SUCCEEDED
	default:
		panic("Unknown worker state")
	}

	// If we need to shut down, we should never be performing
	// blocking Synchronize() calls. We should still let the calls
	// go through, so that we can either finish the current action,
	// or properly ensure the scheduler thinks we're in the idle
	// state.
	if ctx.Err() != nil {
		c.request.PreferBeingIdle = true
		ctx = context.Background()
	}

	// Inform scheduler of current worker state, potentially
	// requesting new work. If this fails, we might have lost an
	// execute request sent by the scheduler, so assume the
	// scheduler thinks we may be executing.
	response, err := c.scheduler.Synchronize(ctx, &c.request)
	if c.schedulerMayThinkExecutingUntil == nil {
		c.touchSchedulerMayThinkExecuting()
	}
	if err != nil {
		return false, util.StatusWrap(err, "Failed to synchronize with scheduler")
	}

	// Determine when we should contact the scheduler again in case
	// of no activity.
	nextSynchronizationAt := response.NextSynchronizationAt
	if err := nextSynchronizationAt.CheckValid(); err != nil {
		return false, util.StatusWrap(err, "Scheduler response contained invalid synchronization timestamp")
	}
	c.nextSynchronizationAt = nextSynchronizationAt.AsTime()

	// As Synchronize() succeeded, the next call does not need to
	// include data associated with the current execution event.
	if workerState, ok := c.request.CurrentState.WorkerState.(*remoteworker_pb.CurrentState_Executing_); ok {
		workerState.Executing.Event = nil
	}

	// Apply desired state changes provided by the scheduler.
	if desiredState := response.DesiredState; desiredState != nil {
		switch workerState := desiredState.WorkerState.(type) {
		case *remoteworker_pb.DesiredState_VerifyingPublicKeys_:
			// Scheduler wants us to recompute our
			// verification zeros.
			verificationPublicKey, err := x509.ParsePKIXPublicKey(workerState.VerifyingPublicKeys.VerificationPkixPublicKey)
			if err != nil {
				return false, util.StatusWrapWithCode(err, codes.Internal, "Scheduler provided an invalid verification PKIX public key")
			}
			verificationECDHPublicKey, ok := verificationPublicKey.(*ecdh.PublicKey)
			if !ok {
				return false, status.Error(codes.Internal, "Scheduler provided an verification PKIX public key that is not an ECDH public key")
			}

			for i, privateKey := range c.privateKeys {
				sharedSecret, err := privateKey.ECDH(verificationECDHPublicKey)
				if err != nil {
					return false, util.StatusWrapfWithCode(err, codes.Internal, "Failed to compute shared secret for PKIX public key %s", base64.StdEncoding.EncodeToString(c.request.PublicKeys[i].PkixPublicKey))
				}
				aesCipher, err := aes.NewCipher(sharedSecret)
				if err != nil {
					return false, util.StatusWrapfWithCode(err, codes.Internal, "Failed to create block cipher for PKIX public key %s", base64.StdEncoding.EncodeToString(c.request.PublicKeys[i].PkixPublicKey))
				}
				verificationZeros := make([]byte, aesCipher.BlockSize())
				aesCipher.Encrypt(verificationZeros, verificationZeros)
				c.request.PublicKeys[i].VerificationZeros = verificationZeros
			}
		case *remoteworker_pb.DesiredState_Idle:
			// Scheduler is forcing us to go back to idle.
			c.stopExecution()
			c.schedulerMayThinkExecutingUntil = nil
			return true, nil
		case *remoteworker_pb.DesiredState_Executing_:
			// Scheduler is requesting us to execute the
			// next action, maybe forcing us to to stop
			// execution of the current build action.
			if err := c.startExecution(workerState.Executing); err != nil {
				c.request.CurrentState.WorkerState = &remoteworker_pb.CurrentState_Rejected_{
					Rejected: &remoteworker_pb.CurrentState_Rejected{
						TaskUuid: workerState.Executing.TaskUuid,
						Reason:   status.Convert(err).Proto(),
					},
				}
			}
			c.touchSchedulerMayThinkExecuting()
			return false, nil
		default:
			return false, status.Error(codes.Internal, "Scheduler provided an unknown desired state")
		}
	}

	// Scheduler has instructed to continue as is.
	if currentStateIsExecuting {
		c.touchSchedulerMayThinkExecuting()
		return false, nil
	}
	c.schedulerMayThinkExecutingUntil = nil
	return true, nil
}

// LaunchWorkerThread launches a single routine that uses a build client
// to repeatedly synchronizes against the scheduler, requesting a task
// to execute.
func LaunchWorkerThread(group program.Group, run func(ctx context.Context) (bool, error), workerName string) {
	group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		generator := random.NewFastSingleThreadedGenerator()
		for {
			terminationStartedBeforeRun := ctx.Err() != nil
			if mayTerminate, err := run(ctx); mayTerminate && ctx.Err() != nil {
				log.Printf("Worker %s: terminating", workerName)
				return nil
			} else if err != nil {
				log.Printf("Worker %s: %s", workerName, err)

				// In case of errors, sleep a random amount of
				// time. Allow the sleep to be skipped once when
				// termination is initiated, so that it happens
				// quickly.
				if d := random.Duration(generator, 5*time.Second); terminationStartedBeforeRun {
					time.Sleep(d)
				} else {
					t := time.NewTimer(d)
					select {
					case <-t.C:
					case <-ctx.Done():
						t.Stop()
					}
				}
			}
		}
	})
}

// ParsePlatformPrivateKeys parses a set of ECDH private keys specified
// in a worker's configuration file, so that they can be provided to
// NewClient().
func ParsePlatformPrivateKeys(privateKeys []string) ([]*ecdh.PrivateKey, error) {
	platformPrivateKeys := make([]*ecdh.PrivateKey, 0, len(privateKeys))
	for i, privateKey := range privateKeys {
		privateKeyBlock, _ := pem.Decode([]byte(privateKey))
		if privateKeyBlock == nil {
			return nil, status.Errorf(codes.InvalidArgument, "Platform private key at index %d does not contain a PEM block", i)
		}
		if privateKeyBlock.Type != "PRIVATE KEY" {
			return nil, status.Errorf(codes.InvalidArgument, "PEM block of platform private key at index %d is not of type PRIVATE KEY", i)
		}
		parsedPrivateKey, err := x509.ParsePKCS8PrivateKey(privateKeyBlock.Bytes)
		if err != nil {
			return nil, util.StatusWrapf(err, "Failed to parse platform private key at index %d", i)
		}
		ecdhPrivateKey, ok := parsedPrivateKey.(*ecdh.PrivateKey)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Platform private key at index %d is not an ECDH private key", i)
		}
		platformPrivateKeys = append(platformPrivateKeys, ecdhPrivateKey)
	}
	return platformPrivateKeys, nil
}

// ParseClientCertificateAuthorities converts a series of X.509 in PEM
// blocks to a certificate pool, so that it can be provided to
// NewClient().
func ParseClientCertificateAuthorities(certificateAuthorities string) (*x509.CertPool, error) {
	clientCertificateAuthorities := x509.NewCertPool()
	for certificateBlock, remainder := pem.Decode([]byte(certificateAuthorities)); certificateBlock != nil; certificateBlock, remainder = pem.Decode(remainder) {
		if certificateBlock.Type != "CERTIFICATE" {
			return nil, status.Error(codes.InvalidArgument, "Client certificate authority is not of type CERTIFICATE")
		}
		certificate, err := x509.ParseCertificate(certificateBlock.Bytes)
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid certificate in client certificate authorities")
		}
		clientCertificateAuthorities.AddCert(certificate)
	}
	return clientCertificateAuthorities, nil
}
