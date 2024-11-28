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
  executionGrpcClient: {
    address: 'unix://%s/playground_scheduler_clients.sock' % statePath,
  },
  executionClientPrivateKey: |||
    -----BEGIN PRIVATE KEY-----
    MC4CAQAwBQYDK2VuBCIEIBPNQt1DBT5mnFKhDGtUU7WVcmIytjDNzOKh1kHNiurp
    -----END PRIVATE KEY-----
  |||,
  executionClientCertificateChain: |||
    -----BEGIN CERTIFICATE-----
    MIIBLjCB4aADAgECAgEBMAUGAytlcDBZMQswCQYDVQQGEwJBVTETMBEGA1UECAwK
    U29tZS1TdGF0ZTEhMB8GA1UECgwYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMRIw
    EAYDVQQDDAlDbGllbnQgQ0EwIhgPMDAwMTAxMDEwMDAwMDBaGA8wMDAxMDEwMTAw
    MDAwMFowADAqMAUGAytlbgMhALK43w91tDyII8MSKR7iiB+9gLvmmv/MBME3XOmD
    75F1oyMwITAfBgNVHSMEGDAWgBSzjAKHNtBRvnPH4BoIU4doYjwH8DAFBgMrZXAD
    QQBai74zE6A2yejtbOEvxUfwmwUZIScP4weRJh7aR3ZJkQNtM9UI7aQW1LMndErj
    KFxMViMC1b06ZRZ/Vys023wC
    -----END CERTIFICATE-----
  |||,
}
