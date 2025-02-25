local statePath = std.extVar('STATE_PATH');

{
  storageGrpcClient: {
    address: 'unix://%s/bonanza_storage_frontend.sock' % statePath,
  },
  schedulerGrpcClient: {
    address: 'unix://%s/bonanza_scheduler_workers.sock' % statePath,
  },
  filePool: { blockDevice: { file: {
    path: statePath + '/bonanza_worker_filepool',
    sizeBytes: 1e9,
  } } },
  buildDirectories: [{
    runners: [{
      endpoint: {
        address: 'unix://%s/bb_runner.sock' % statePath,
      },
      concurrency: 1,
      platformPrivateKeys: [
        |||
          -----BEGIN PRIVATE KEY-----
          MC4CAQAwBQYDK2VuBCIEIOgTxCpcEulRlM07gyhE3ydECoP915a6wj6SR6W62QZ4
          -----END PRIVATE KEY-----
        |||,
      ],
      clientCertificateAuthorities: |||
        -----BEGIN CERTIFICATE-----
        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
        -----END CERTIFICATE-----
      |||,
      sizeClass: 1,
      isLargestSizeClass: true,
      maximumExecutionTimeoutCompensation: '3600s',
      maximumWritableFileUploadDelay: '60s',
      maximumFilePoolFileCount: 10000,
      maximumFilePoolSizeBytes: 1e9,
      workerId: { host: 'localhost' },
    }],
    mount: {
      mountPath: statePath + '/bonanza_worker_mount',
      nfsv4: {
        darwin: {
          socketPath: statePath + '/bonanza_worker_mount.sock',
        },
        enforcedLeaseTime: '120s',
        announcedLeaseTime: '60s',
      },
    },
  }],
}
