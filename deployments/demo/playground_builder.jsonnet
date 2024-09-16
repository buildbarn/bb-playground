local statePath = std.extVar('STATE_PATH');

{
  grpcServers: [{
    listenPaths: [statePath + '/playground_builder.sock'],
    authenticationPolicy: { allow: {} },
  }],
  storageGrpcClient: {
    address: 'unix://%s/playground_storage_frontend.sock' % statePath,
  },
}
