local statePath = std.extVar('STATE_PATH');

{
  global: { diagnosticsHttpServer: {
    httpServers: [{
      listenAddresses: [':9980'],
      authenticationPolicy: { allow: {} },
    }],
    enablePrometheus: true,
    enablePprof: true,
  } },
  grpcServers: [{
    listenPaths: [statePath + '/playground_builder.sock'],
    authenticationPolicy: { allow: {} },
  }],
  storageGrpcClient: {
    address: 'unix://%s/playground_storage_frontend.sock' % statePath,
  },
  cacheDirectoryPath: statePath + '/playground_builder_cache',
  filePool: { directoryPath: statePath + '/playground_builder_filepool' },
}
