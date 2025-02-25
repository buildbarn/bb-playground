local statePath = std.extVar('STATE_PATH');

{
  buildDirectoryPath: statePath + '/bonanza_worker_mount',
  grpcServers: [{
    listenPaths: [statePath + '/bb_runner.sock'],
    authenticationPolicy: { allow: {} },
  }],
}
