local statePath = std.extVar('STATE_PATH');

{
  clientGrpcServers: [{
    listenPaths: [statePath + '/playground_scheduler_clients.sock'],
    authenticationPolicy: { allow: {} },
  }],
  workerGrpcServers: [{
    listenPaths: [statePath + '/playground_scheduler_workers.sock'],
    authenticationPolicy: { allow: {} },
  }],
  actionRouter: {
    simple: {
      initialSizeClassAnalyzer: {
        maximumExecutionTimeout: '7200s',
      },
    },
  },
  platformQueueWithNoWorkersTimeout: '900s',
}
