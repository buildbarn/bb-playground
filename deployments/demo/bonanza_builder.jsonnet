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
    listenPaths: [statePath + '/bonanza_builder.sock'],
    authenticationPolicy: { allow: {} },
  }],
  storageGrpcClient: {
    address: 'unix://%s/bonanza_storage_frontend.sock' % statePath,
  },
  cacheDirectoryPath: statePath + '/bonanza_builder_cache',
  filePool: { directoryPath: statePath + '/bonanza_builder_filepool' },
  executionGrpcClient: {
    address: 'unix://%s/bonanza_scheduler_clients.sock' % statePath,
  },
  executionClientPrivateKey: |||
    -----BEGIN PRIVATE KEY-----
    MC4CAQAwBQYDK2VuBCIEIBVNtenWSDGHkBtGW5gtRjltRRJJgBs5BIG3e6K7/TO/
    -----END PRIVATE KEY-----
  |||,
  executionClientCertificateChain: |||
    -----BEGIN CERTIFICATE-----
    MIIDBTCB7qADAgECAgEBMA0GCSqGSIb3DQEBCwUAMIGGMQswCQYDVQQGEwJYWDES
    MBAGA1UECAwJU3RhdGVOYW1lMREwDwYDVQQHDAhDaXR5TmFtZTEUMBIGA1UECgwL
    Q29tcGFueU5hbWUxGzAZBgNVBAsMEkNvbXBhbnlTZWN0aW9uTmFtZTEdMBsGA1UE
    AwwUQ29tbW9uTmFtZU9ySG9zdG5hbWUwHhcNMjQxMjAzMTkwNjQxWhcNMjUxMjAz
    MTkwNjQxWjAAMCowBQYDK2VuAyEA/aFk6cfHhLSonXUsQ3g/v2kAl87gftLTYvjY
    xFbf2wwwDQYJKoZIhvcNAQELBQADggIBAMbOGzsOog6w3JsehpljIb+gYSI3Km+L
    CtzCunGxMbeuPKWm4lQTxJBzp9Jgko/dOjjwpLAcUVrZ04hnaiD4rtcm8AoNvsU1
    JqOBL+QesS9Y7N+t8cNe8I4R0M6HIhx3TzhpKiu5oVQtHPj1vkRZz5WFdZikig1l
    phJKl1avzRYH5M1s069QRxIhtElV96VZ++tg5d4xRisejKHEorZwNC2rCAYWqTm7
    cd1kbDp6YNhNn9BmiASLi0AdTWgCTXO+diwEabqSNylaxJKDtqZKXa2sklmxuW3+
    wYxHqaznbySzplUdrI+LKlF5mPDqDiNB55cM1FEFHZA1ykTlOsSKMT+l8F5VDSP5
    QDXtS46UYlAkJSqyP+goi+5EotYgLJA9+1/yB8R7rPeDk0ExvMVe0TOk8+pwG9MD
    bfr9pilMFbAyH6MY4aC8bAuY60HrPMp4mSdTyVdK6In/zm5TzY/hWXsJ+FwXEOjg
    0Uhlx/tbtEx34Tmh/7O/zPGTqlMtVYAULdMMmE2G18jb4dztLyR2ybBJ07Wk2RZY
    b2fdM0rdWLC5hpzA6lOY6OcPE0IzaEBlxFh4M1KLmazvgtBEk/O3lgK4nHqB8DEq
    NFOxMVZMoqlkVu8mVvjjmuj7lRvA0z+vFb81vH1csmH4MPf9AFgp8SeFciELKSuY
    cAhYLEjfNOEe
    -----END CERTIFICATE-----
  |||,
}
