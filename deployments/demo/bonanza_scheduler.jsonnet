local statePath = std.extVar('STATE_PATH');

{
  clientGrpcServers: [{
    listenPaths: [statePath + '/bonanza_scheduler_clients.sock'],
    authenticationPolicy: { allow: {} },
  }],
  workerGrpcServers: [{
    listenPaths: [statePath + '/bonanza_scheduler_workers.sock'],
    authenticationPolicy: { allow: {} },
  }],
  actionRouter: {
    simple: {
      initialSizeClassAnalyzer: {
        maximumExecutionTimeout: '86400s',
      },
    },
  },
  platformQueueWithNoWorkersTimeout: '900s',
}
