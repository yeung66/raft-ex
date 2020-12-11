
# [System.Environment]::SetEnvironmentVariable('GOPATH', "${Get-Location}", [System.EnvironmentVariableTarget]::Process)
# [System.Environment]::SetEnvironmentVariable('GO111MODULE', "auto", [System.EnvironmentVariableTarget]::Process)

export GOPATH=$PWD
export GO111MODULE=auto

