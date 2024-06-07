local replicasCount = 4;
local statePath = std.extVar('STATE_PATH');

{
  grpcServers: [{
    listenPaths: [statePath + '/playground_storage_frontend.sock'],
    authenticationPolicy: { allow: {} },
  }],

  objectStoreConcurrency: 100,
  maximumUnfinalizedDagsCount: 100,
  maximumUnfinalizedParentsLimit: {
    count: 1000,
    sizeBytes: 16 * 1024 * 1024,
  },

  grpcClientsShardsReplicaA: [
    { address: 'unix://%s/playground_storage_shard_a%s.sock' % [statePath, replica] }
    for replica in std.range(0, replicasCount - 1)
  ],
  grpcClientsShardsReplicaB: [
    { address: 'unix://%s/playground_storage_shard_b%s.sock' % [statePath, replica] }
    for replica in std.range(0, replicasCount - 1)
  ],
}
