local replicasCount = 4;
local statePath = std.extVar('STATE_PATH');

{
  global: { diagnosticsHttpServer: {
    httpServers: [{
      listenAddresses: [':9981'],
      authenticationPolicy: { allow: {} },
    }],
    enablePrometheus: true,
    enablePprof: true,
  } },
  grpcServers: [{
    listenPaths: [statePath + '/bonanza_storage_frontend.sock'],
    authenticationPolicy: { allow: {} },
  }],

  objectStoreConcurrency: 100,
  maximumUnfinalizedDagsCount: 100,
  maximumUnfinalizedParentsLimit: {
    count: 1000,
    sizeBytes: 16 * 1024 * 1024,
  },

  shardsReplicaA: {
    [std.toString(replica)]: {
      client: { address: 'unix://%s/bonanza_storage_shard_a%s.sock' % [statePath, replica] },
      weight: 1,
    }
    for replica in std.range(0, replicasCount - 1)
  },
  shardsReplicaB: {
    [std.toString(replica)]: {
      client: { address: 'unix://%s/bonanza_storage_shard_b%s.sock' % [statePath, replica] },
      weight: 1,
    }
    for replica in std.range(0, replicasCount - 1)
  },
}
