# Windows equivalent of the Makefile. Usage: .\make.ps1 [proto|build|test|clean]
param([ValidateSet('proto', 'build', 'test', 'clean')][string]$Target = 'build')

$ErrorActionPreference = 'Stop'
$Module = 'github.com/Lord-OfShadows/gfs'
$Protos = 'proto/common.proto', 'proto/coordinator.proto', 'proto/chunkserver.proto'

function Invoke-Step([scriptblock]$Step) {
    & $Step
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

switch ($Target) {
    'proto' {
        Invoke-Step { protoc -I proto --go_out=. "--go_opt=module=$Module" --go-grpc_out=. "--go-grpc_opt=module=$Module" @Protos }
    }
    'build' {
        Invoke-Step { go build ./... }
        Invoke-Step { go build -o bin/ ./cmd/... }
    }
    'test' {
        Invoke-Step { go test ./... }
    }
    'clean' {
        Invoke-Step { go clean }
        if (Test-Path bin) { Remove-Item -Recurse -Force bin }
    }
}
