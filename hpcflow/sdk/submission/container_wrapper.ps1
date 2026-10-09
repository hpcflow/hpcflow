#Requires -Version 7.3
[CmdletBinding(PositionalBinding = $false)]
param(
    [string]$Image = '__APP_NAME_CONTAINER_IMAGE__',
    [string]$Machine = [System.Net.Dns]::GetHostName(),
    [string[]]$ContainerCommand = @('docker'),
    [string[]]$ConfigArgs = @(),
    [string]$ResumeResult,
    [string]$Context,
    [switch]$RunJob,
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$HpcflowArgs = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandArgumentPassing = 'Standard'
$PSNativeCommandUseErrorActionPreference = $false
$root = (Get-Location).ProviderPath
$utf8 = [System.Text.UTF8Encoding]::new($false)
$appPrefix = '__APP_NAME_'.Trim('_')
$configEnvName = "${appPrefix}_CONFIG_DIR"
$hostConfigDirectory = $null
$containerConfigDirectory = $null
if ($Context) {
    $hostContext = Get-Content -Raw -LiteralPath $Context | ConvertFrom-Json -AsHashtable
    $root = $hostContext.mount_root
    $Image = $hostContext.image
    $Machine = $hostContext.machine
    $ContainerCommand = @($hostContext.container_command)
    $ConfigArgs = @($hostContext.config_args)
    if ($hostContext.ContainsKey('container_config_directory')) {
        $hostConfigDirectory = $hostContext.container_config_directory
    }
}

function ConvertTo-ContainerPath {
    param([string]$Value)
    if ($hostConfigDirectory -and $containerConfigDirectory) {
        if ($Value -eq $hostConfigDirectory) { return $containerConfigDirectory }
        if ($Value.StartsWith($hostConfigDirectory.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar)) {
            return $containerConfigDirectory + '/' + $Value.Substring($hostConfigDirectory.Length).TrimStart('\', '/').Replace('\', '/')
        }
    }
    if ($Value -eq $root) { return '/work' }
    if ($Value.StartsWith($root.TrimEnd('\', '/') + [System.IO.Path]::DirectorySeparatorChar)) {
        return '/work/' + $Value.Substring($root.Length).TrimStart('\', '/').Replace('\', '/')
    }
    return $Value
}

function Invoke-Container {
    param([string[]]$CommandArgs, [string]$InputJson, [switch]$Capture)
    $hostEnv = @()
    $configMount = @()
    $configPath = [System.Environment]::GetEnvironmentVariable($configEnvName)
    if ($IsWindows -and $configPath -and
        ($configPath -eq '/work' -or $configPath.StartsWith('/work/')) -and
        -not $hostConfigDirectory) {
        # Preserve an explicitly container-visible configuration path.
    } else {
        if (-not $hostConfigDirectory) {
            if (-not $configPath) {
                $configPath = Join-Path $HOME ".$($appPrefix.ToLowerInvariant())"
            } elseif ($configPath -eq '~' -or $configPath.StartsWith('~/') -or
                $configPath.StartsWith('~\')) {
                $configPath = Join-Path $HOME $configPath.Substring(1).TrimStart('\', '/')
            }
            $script:hostConfigDirectory = [System.IO.Path]::GetFullPath($configPath).TrimEnd('\', '/') + '-container'
        }
        if ($hostConfigDirectory.Contains(',')) {
            throw 'Container configuration directory must not contain a comma.'
        }
        [System.IO.Directory]::CreateDirectory($hostConfigDirectory) | Out-Null
        $configPath = "/config/.$($appPrefix.ToLowerInvariant())-container"
        $configMount = @('--mount', "type=bind,source=$hostConfigDirectory,target=$configPath")
    }
    $script:containerConfigDirectory = $configPath.TrimEnd('/')
    $hostEnv += @(
        '--env', "${configEnvName}=$configPath",
        '--env', "XDG_CACHE_HOME=$containerConfigDirectory/cache",
        '--env', "XDG_DATA_HOME=$containerConfigDirectory/data"
    )
    if ($IsWindows) {
        $hostEnv += @(
            '--env', "${appPrefix}_CONTAINER=$Image",
            '--env', "${appPrefix}_CONTAINER_HOST_OS=nt",
            '--env', "${appPrefix}_CONTAINER_HOST_HOSTNAME=$([System.Net.Dns]::GetHostName())",
            '--env', "${appPrefix}_CONTAINER_HOST_CPU_ARCH=$env:PROCESSOR_ARCHITECTURE"
        )
        foreach ($item in Get-ChildItem Env:) {
            if ($item.Name.StartsWith("${appPrefix}_") -and
                $item.Name -ne $configEnvName -and
                $item.Name -notmatch '_CONTAINER|_RUN_PORT$') {
                $hostEnv += @('--env', "$($item.Name)=$(ConvertTo-ContainerPath $item.Value)")
            }
        }
        $CommandArgs = @(
            '--with-config', 'shells.powershell.defaults.executable',
            (Get-Command pwsh -CommandType Application).Source
        ) + $CommandArgs
    }
    $mappedArgs = @($ConfigArgs + $CommandArgs | ForEach-Object {
        ConvertTo-ContainerPath $_
    })
    $dockerArgs = @(
        'run', '--rm', '-i', '--mount', "type=bind,source=$root,target=/work",
        '--workdir', '/work'
    ) + $configMount + $hostEnv + @($Image) + $mappedArgs
    $exe = $ContainerCommand[0]
    $prefix = @($ContainerCommand | Select-Object -Skip 1)
    if ($PSBoundParameters.ContainsKey('InputJson')) {
        $out = $InputJson | & $exe @prefix @dockerArgs
    } elseif ($Capture) {
        $out = & $exe @prefix @dockerArgs
    } else {
        & $exe @prefix @dockerArgs
        $script:containerExitCode = $LASTEXITCODE
        return
    }
    if ($LASTEXITCODE -ne 0) {
        throw "Container command failed with exit code $LASTEXITCODE."
    }
    return ($out -join "`n")
}

function Get-HostPath {
    param([string]$ContainerPath, [switch]$AllowConfig)
    $hostRoot = $root
    $containerRoot = '/work'
    if ($AllowConfig -and $hostConfigDirectory -and $containerConfigDirectory -and
        ($ContainerPath -eq $containerConfigDirectory -or
         $ContainerPath.StartsWith($containerConfigDirectory + '/'))) {
        $hostRoot = $hostConfigDirectory
        $containerRoot = $containerConfigDirectory
    }
    if ($ContainerPath -ne $containerRoot -and -not $ContainerPath.StartsWith($containerRoot + '/')) {
        throw "Container path is outside /work: $ContainerPath"
    }
    $relative = $ContainerPath.Substring($containerRoot.Length).TrimStart('/')
    if ($relative.Contains('\') -or @($relative.Split('/')) -contains '..') {
        throw "Invalid container path: $ContainerPath"
    }
    return [System.IO.Path]::GetFullPath((Join-Path $hostRoot $relative))
}

function Save-Record {
    param([string]$Path, [object]$Record)
    $json = $Record | ConvertTo-Json -Depth 30
    $temporary = $Path + '.tmp'
    $bytes = $utf8.GetBytes($json + "`n")
    $stream = [System.IO.File]::Open($temporary, [System.IO.FileMode]::Create)
    try {
        $stream.Write($bytes)
        $stream.Flush($true)
    } finally {
        $stream.Dispose()
    }
    [System.IO.File]::Move($temporary, $Path, $true)
}

function Confirm-Result {
    param([hashtable]$Envelope)
    $json = $Envelope.result | ConvertTo-Json -Depth 30 -Compress
    $response = Invoke-Container -CommandArgs @(
        '--with-config', 'machine', $Envelope.result.submit_machine,
        'internal', 'workflow', $Envelope.workflow_path, 'record-host-submission', '-'
    ) -InputJson $json
    $confirmed = ConvertFrom-Json -AsHashtable $response
    if (
        -not $confirmed.ContainsKey('recorded') -or
        $confirmed.recorded -isnot [bool]
    ) {
        throw 'Invalid acknowledgement response from hpcflow.'
    }
}

function Get-DirectDependency {
    param([hashtable]$Dependency, [string]$Reference, [string]$WorkflowHostPath)
    $proc = Get-Process -Id ([int]$Reference) -ErrorAction SilentlyContinue
    if ($null -eq $proc) { return }
    $depContextPath = Join-Path $WorkflowHostPath (
        ".${appPrefix}-host-$($Dependency.submission_index)-$($Dependency.jobscript_index).json"
    )
    if (-not (Test-Path -LiteralPath $depContextPath)) {
        throw 'Cannot verify a running direct dependency not launched by this wrapper.'
    }
    $depContext = Get-Content -Raw -LiteralPath $depContextPath | ConvertFrom-Json -AsHashtable
    if ($depContext.process_id -eq [int]$Reference -and
        $depContext.start_ticks -eq $proc.StartTime.ToUniversalTime().Ticks) {
        return @{ process_id = [int]$Reference; start_ticks = $depContext.start_ticks }
    }
}

function Invoke-HostRun {
    param([string[]]$RunArgs)
    $planJson = Invoke-Container -CommandArgs ($RunArgs + '--containerised') -Capture
    $runPlan = ConvertFrom-Json -AsHashtable $planJson
    if ($null -eq $runPlan) { return }
    if (($runPlan.schema_version -isnot [int] -and $runPlan.schema_version -isnot [long]) -or
        $runPlan.schema_version -ne 1 -or $runPlan.command -isnot [array] -or
        -not $runPlan.command.Count -or $runPlan.environment -isnot [hashtable]) {
        throw 'Invalid host run plan.'
    }
    if ($Context -and $runPlan.workflow_id -ne $hostContext.workflow_id) {
        throw 'Host run plan belongs to a different workflow.'
    }
    $workingDirectory = Get-HostPath $runPlan.working_directory
    $command = @($runPlan.command | ForEach-Object {
        if ($_ -isnot [string] -or $_.Contains([char]0)) {
            throw 'Invalid host command argument.'
        }
        if ($_ -eq '/work' -or $_.StartsWith('/work/') -or
            ($hostConfigDirectory -and ($_ -eq $containerConfigDirectory -or
             $_.StartsWith($containerConfigDirectory + '/')))) {
            Get-HostPath $_ -AllowConfig
        } else { $_ }
    })
    $workflowHostPath = Get-HostPath (ConvertTo-ContainerPath $RunArgs[2])
    $runJournal = Join-Path $workflowHostPath (
        ".${appPrefix}-run-" + ($RunArgs[4..8] -join '-') + '.json'
    )
    if (Test-Path -LiteralPath $runJournal) {
        throw "A host run journal exists: $runJournal. Reconcile it before retrying."
    }
    Save-Record $runJournal @{ state = 'launching'; run_args = $RunArgs; command = $command }
    $previous = @{}
    try {
        foreach ($key in $runPlan.environment.Keys) {
            if ([string]::IsNullOrWhiteSpace($key) -or $key.Contains('=') -or
                $key.Contains([char]0) -or $key -match "_CONTAINER|^${appPrefix}_RUN_PORT$") {
                throw "Invalid execution environment variable: $key"
            }
            $previous[$key] = [Environment]::GetEnvironmentVariable($key)
            $value = [string]$runPlan.environment[$key]
            if ($value -eq '/work' -or $value.StartsWith('/work/') -or
                ($hostConfigDirectory -and ($value -eq $containerConfigDirectory -or
                 $value.StartsWith($containerConfigDirectory + '/')))) {
                $value = Get-HostPath $value -AllowConfig
            }
            [Environment]::SetEnvironmentVariable($key, $value)
        }
        Push-Location -LiteralPath $workingDirectory
        $launchError = $null
        try {
            $exe = $command[0]
            $commandArgs = @($command | Select-Object -Skip 1)
            & $exe @commandArgs
            $runExitCode = $LASTEXITCODE
        } catch {
            $launchError = $_
            $runExitCode = 1
        } finally {
            Pop-Location
        }
    } finally {
        foreach ($key in $previous.Keys) {
            [Environment]::SetEnvironmentVariable($key, $previous[$key])
        }
    }
    $completeArgs = @($RunArgs)
    $completeArgs[3] = 'complete-host-run'
    $completeArgs += @('--', [string]$runExitCode)
    Save-Record $runJournal @{
        state = 'executed'
        complete_args = $completeArgs
        exit_code = $runExitCode
    }
    $response = Invoke-Container -CommandArgs $completeArgs -Capture |
        ConvertFrom-Json -AsHashtable
    if ($response.completed -isnot [bool] -or -not $response.completed) {
        throw 'Invalid run completion response.'
    }
    Remove-Item -LiteralPath $runJournal
    if ($null -ne $launchError) { throw $launchError }
}

try {
    if (
        -not $ContainerCommand.Count -or [string]::IsNullOrWhiteSpace($Image) -or
        $Image -match '^__APP_NAME_.*__$'
    ) {
        throw 'Image and ContainerCommand must not be empty.'
    }
    if ($root.Contains(',')) { throw 'The mounted directory must not contain a comma.' }
    if ($RunJob) {
        if (-not $Context) { throw 'RunJob requires Context.' }
        $gate = $hostContext.gate
        while (-not (Test-Path -LiteralPath $gate)) { Start-Sleep -Milliseconds 100 }
        foreach ($dep in $hostContext.dependencies) {
            $proc = Get-Process -Id $dep.process_id -ErrorAction SilentlyContinue
            if ($null -ne $proc -and $proc.StartTime.ToUniversalTime().Ticks -eq $dep.start_ticks) {
                $proc.WaitForExit()
            }
        }
        [Environment]::SetEnvironmentVariable("${appPrefix}_CONTAINER_WRAPPER", $PSCommandPath)
        [Environment]::SetEnvironmentVariable("${appPrefix}_CONTAINER_CONTEXT", $Context)
        Set-Location -LiteralPath $hostContext.workflow_host_path
        $exe = $hostContext.command[0]
        $jobArgs = @($hostContext.command | Select-Object -Skip 1)
        & $exe @jobArgs 1> $hostContext.stdout_path 2> $hostContext.stderr_path
        exit $LASTEXITCODE
    }
    if ($ResumeResult) {
        if ($HpcflowArgs.Count) { throw 'ResumeResult cannot be combined with HpcflowArgs.' }
        $envelope = Get-Content -Raw -LiteralPath $ResumeResult | ConvertFrom-Json -AsHashtable
        if ($envelope.state -ne 'submitted') {
            throw 'The launch outcome is uncertain. Inspect the journal and scheduler before retrying.'
        }
        if ($envelope.mount_root -ne $root -or $envelope.image -ne $Image) {
            throw 'Recovery must use the original mount root and container image.'
        }
        $null = Get-HostPath $envelope.workflow_path
        Confirm-Result $envelope
        if ($envelope.ContainsKey('gate')) {
            [System.IO.File]::WriteAllText($envelope.gate, 'acknowledged', $utf8)
        }
        Remove-Item -LiteralPath $ResumeResult
        Write-Output 'Host submission acknowledged; no job was launched.'
        exit 0
    }
    if (-not $HpcflowArgs.Count) { throw 'Supply HpcflowArgs or ResumeResult.' }
    if ($Context) {
        $offset = 0
        while ($offset -lt $HpcflowArgs.Count -and $HpcflowArgs[$offset].StartsWith('-')) {
            $option = $HpcflowArgs[$offset]
            $count = switch ($option) {
                '--with-config' { 3 }
                { $_ -in @('--std-stream', '--config-dir', '--config-key', '--timeit-file') } { 2 }
                '--timeit' { 1 }
                default { throw "Unsupported internal global option: $option" }
            }
            if ($offset + $count -ge $HpcflowArgs.Count) { throw "Missing command after $option." }
            $ConfigArgs += $HpcflowArgs[$offset..($offset + $count - 1)]
            $offset += $count
        }
        $HpcflowArgs = @($HpcflowArgs | Select-Object -Skip $offset)
    }
    if ($HpcflowArgs.Count -ge 4 -and $HpcflowArgs[0] -eq 'internal' -and
        $HpcflowArgs[1] -eq 'workflow' -and $HpcflowArgs[3] -eq 'execute-run') {
        Invoke-HostRun $HpcflowArgs
        exit 0
    }
    if ($HpcflowArgs[0].StartsWith('-') -and $HpcflowArgs.Count -gt 1) {
        throw 'Supply global hpcflow options via ConfigArgs, not before the command in HpcflowArgs.'
    }
    $workflowCommandIndex = 2
    if ($HpcflowArgs[0] -eq 'workflow') {
        while ($workflowCommandIndex -lt $HpcflowArgs.Count) {
            $word = $HpcflowArgs[$workflowCommandIndex]
            if ($word -in @('--ref-type', '-r')) { $workflowCommandIndex += 2 }
            elseif ($word.StartsWith('--ref-type=')) { $workflowCommandIndex++ }
            else { break }
        }
    }
    $submission = (
        $HpcflowArgs[0] -eq 'go' -or
        ($HpcflowArgs.Count -ge 2 -and $HpcflowArgs[0] -eq 'demo-workflow' -and $HpcflowArgs[1] -eq 'go') -or
        ($workflowCommandIndex -lt $HpcflowArgs.Count -and $HpcflowArgs[0] -eq 'workflow' -and $HpcflowArgs[$workflowCommandIndex] -eq 'submit')
    )
    if (-not $submission -and (
        ($HpcflowArgs[0] -eq 'workflow' -and $HpcflowArgs -contains 'submit') -or
        ($HpcflowArgs[0] -eq 'demo-workflow' -and $HpcflowArgs -contains 'go')
    )) {
        throw 'Unsupported submission argument order; use the documented command-first invocation.'
    }
    if (-not $submission) {
        if ($IsWindows -and $HpcflowArgs[0] -eq 'workflow' -and
            $workflowCommandIndex -lt $HpcflowArgs.Count -and
            $HpcflowArgs[$workflowCommandIndex] -in @('wait', 'cancel', 'abort-run')) {
            throw 'Host process monitoring and cancellation are not yet supported by this wrapper.'
        }
        Invoke-Container -CommandArgs $HpcflowArgs
        exit $script:containerExitCode
    }
    if ([string]::IsNullOrWhiteSpace($Machine)) {
        throw 'Machine must not be empty; omit it to use the host hostname.'
    }
    if ($HpcflowArgs -contains '--wait' -or $HpcflowArgs -contains '--cancel') {
        throw 'Containerised submission cannot wait for or cancel jobs.'
    }
    $prepareArgs = @('--with-config', 'machine', $Machine) + $HpcflowArgs
    if ($HpcflowArgs -notcontains '--containerised') { $prepareArgs += '--containerised' }
    $plan = Invoke-Container -CommandArgs $prepareArgs -Capture |
        ConvertFrom-Json -AsHashtable
    if (
        $plan.schema_version -isnot [long] -and $plan.schema_version -isnot [int] -or
        $plan.schema_version -ne 1 -or $plan.workflow_id -isnot [string] -or
        [string]::IsNullOrWhiteSpace($plan.workflow_id) -or $plan.jobscripts -isnot [array]
    ) {
        throw 'Invalid host submission plan.'
    }
    $workflowPath = Get-HostPath $plan.workflow_path
    if (-not (Test-Path -LiteralPath $workflowPath -PathType Container)) {
        throw "Host workflow directory does not exist: $workflowPath"
    }
    $journalPath = Join-Path $workflowPath '.hpcflow-host-submission.json'
    if (Test-Path -LiteralPath $journalPath) {
        throw "A host launch journal exists: $journalPath. Recover it before submitting again."
    }
    $refs = @{}
    $seen = @{}
    # Validate the whole plan before launching its first job.
    foreach ($job in $plan.jobscripts) {
        if (
            $job.submission_index -isnot [long] -and $job.submission_index -isnot [int] -or
            $job.jobscript_index -isnot [long] -and $job.jobscript_index -isnot [int] -or
            $job.submission_index -lt 0 -or $job.jobscript_index -lt 0
        ) { throw 'Invalid jobscript indices.' }
        $key = "$($job.submission_index):$($job.jobscript_index)"
        if ($seen.ContainsKey($key)) { throw "Duplicate jobscript: $key" }
        if ($job.scheduler -notin @('slurm', 'sge', 'direct')) {
            throw "Unsupported scheduler: $($job.scheduler)"
        }
        if ($job.scheduler -eq 'direct') {
            if (-not $IsWindows -or $job.shell -ne 'powershell' -or $job.is_array) {
                throw 'Direct container execution currently requires Windows PowerShell and non-array jobs.'
            }
            foreach ($pathKey in @('stdout_path', 'stderr_path')) {
                if (-not $job.ContainsKey($pathKey) -or $job[$pathKey] -isnot [string] -or
                    [string]::IsNullOrWhiteSpace($job[$pathKey]) -or $job[$pathKey].StartsWith('/')) {
                    throw "Invalid direct $pathKey."
                }
                $null = Get-HostPath ($plan.workflow_path.TrimEnd('/') + '/' + $job[$pathKey])
            }
        }
        if ($job.path -isnot [string] -or $job.path.StartsWith('/') -or $job.path.Contains('\')) {
            throw 'Jobscript paths must be relative slash-separated paths.'
        }
        $hostScript = Get-HostPath ($plan.workflow_path.TrimEnd('/') + '/' + $job.path)
        if (-not (Test-Path -LiteralPath $hostScript -PathType Leaf)) {
            throw "Host jobscript does not exist: $hostScript"
        }
        if ($job.submit_command -isnot [array] -or -not $job.submit_command.Count) {
            throw 'Invalid submit_command argument list.'
        }
        foreach ($arg in $job.submit_command) {
            if ($arg -isnot [string] -or $arg.Contains([char]0)) {
                throw 'submit_command must contain argument strings without NUL bytes.'
            }
        }
        if ([string]::IsNullOrWhiteSpace($job.submit_command[0])) {
            throw 'Missing submission executable.'
        }
        if ($job.dependencies -isnot [array]) { throw 'Invalid dependencies list.' }
        if (-not ($job.submit_command -match '__APP_NAME_JOBSCRIPT_PATH__')) {
            throw 'Submission command does not contain the jobscript path placeholder.'
        }
        $tokens = @('__APP_NAME_JOBSCRIPT_PATH__')
        foreach ($dep in $job.dependencies) {
            $depKey = "$($dep.submission_index):$($dep.jobscript_index)"
            if (
                ($dep.submission_index -isnot [long] -and $dep.submission_index -isnot [int]) -or
                ($dep.jobscript_index -isnot [long] -and $dep.jobscript_index -isnot [int]) -or
                $dep.submission_index -lt 0 -or $dep.jobscript_index -lt 0 -or
                $dep.placeholder -ne "__APP_NAME_JOB_$($dep.submission_index)_$($dep.jobscript_index)__" -or
                ($null -eq $dep.reference -and -not $seen.ContainsKey($depKey))
            ) { throw "Unresolved or out-of-order dependency: $depKey" }
            if ($null -ne $dep.reference -and (
                $dep.reference -isnot [string] -or $dep.reference -notmatch '^\d+$'
            )) { throw "Invalid dependency reference: $depKey" }
            if ($job.scheduler -eq 'direct' -and $null -ne $dep.reference) {
                $null = Get-DirectDependency $dep $dep.reference $workflowPath
            }
            $tokens += $dep.placeholder
        }
        foreach ($arg in $job.submit_command) {
            foreach ($token in $tokens) { $arg = $arg.Replace($token, '') }
            if ($arg -match '__APP_NAME_(JOBSCRIPT_PATH|JOB_\d+_\d+)__') {
                throw "Unknown placeholder in submit_command: $arg"
            }
        }
        $seen[$key] = $true
    }
    Push-Location -LiteralPath $workflowPath
    try {
        foreach ($job in $plan.jobscripts) {
            $hostScript = Get-HostPath ($plan.workflow_path.TrimEnd('/') + '/' + $job.path)
            $command = @($job.submit_command | ForEach-Object {
                $arg = $_.Replace('__APP_NAME_JOBSCRIPT_PATH__', $hostScript)
                foreach ($dep in $job.dependencies) {
                    $ref = $dep.reference
                    if ($null -eq $ref) { $ref = $refs["$($dep.submission_index):$($dep.jobscript_index)"] }
                    $arg = $arg.Replace($dep.placeholder, [string]$ref)
                }
                if ($arg -match '__APP_NAME_(JOBSCRIPT_PATH|JOB_\d+_\d+)__') {
                    throw "Unresolved token in command: $arg"
                }
                $arg
            })
            $result = @{
                schema_version = 1
                workflow_id = $plan.workflow_id
                submission_index = $job.submission_index
                jobscript_index = $job.jobscript_index
                submit_command = $command
                submit_time = [DateTimeOffset]::UtcNow.ToString('o')
                submit_hostname = [System.Net.Dns]::GetHostName()
                submit_machine = $Machine
            }
            $envelope = @{
                state = 'launching'
                mount_root = $root
                image = $Image
                workflow_path = $plan.workflow_path
                result = $result
            }
            # A surviving intent record blocks automatic relaunch after an uncertain failure.
            $stream = [System.IO.File]::Open($journalPath, [System.IO.FileMode]::CreateNew)
            $stream.Dispose()
            Save-Record $journalPath $envelope
            if ($job.scheduler -eq 'direct') {
                $dependencies = @(
                    foreach ($dep in $job.dependencies) {
                        $pidValue = $dep.reference
                        if ($null -eq $pidValue) { $pidValue = $refs["$($dep.submission_index):$($dep.jobscript_index)"] }
                        Get-DirectDependency $dep ([string]$pidValue) $workflowPath
                    }
                )
                $prefix = ".${appPrefix}-host-$($job.submission_index)-$($job.jobscript_index)"
                $contextPath = Join-Path $workflowPath ($prefix + '.json')
                $gate = Join-Path $workflowPath ($prefix + '.ack')
                if (Test-Path -LiteralPath $gate) { throw "Acknowledgement gate already exists: $gate" }
                $wrapperPath = Join-Path $workflowPath ".$($appPrefix.ToLowerInvariant()).ps1"
                if ($PSCommandPath -ne $wrapperPath) {
                    Copy-Item -LiteralPath $PSCommandPath -Destination $wrapperPath
                }
                $stdoutPath = Get-HostPath ($plan.workflow_path.TrimEnd('/') + '/' + $job.stdout_path)
                $stderrPath = Get-HostPath ($plan.workflow_path.TrimEnd('/') + '/' + $job.stderr_path)
                Save-Record $contextPath @{
                    mount_root = $root
                    image = $Image
                    machine = $Machine
                    container_command = $ContainerCommand
                    config_args = $ConfigArgs
                    container_config_directory = $hostConfigDirectory
                    workflow_host_path = $workflowPath
                    workflow_id = $plan.workflow_id
                    command = $command
                    dependencies = $dependencies
                    gate = $gate
                    stdout_path = $stdoutPath
                    stderr_path = $stderrPath
                }
                $quotedWrapper = "'" + $wrapperPath.Replace("'", "''") + "'"
                $quotedContext = "'" + $contextPath.Replace("'", "''") + "'"
                $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes(
                    "& $quotedWrapper -Context $quotedContext -RunJob"
                ))
                $launchExe = (Get-Command pwsh -CommandType Application).Source
                $launchArgs = @('-NoProfile', '-EncodedCommand', $encoded)
                $result.submit_command = @($launchExe) + $launchArgs
                $envelope.gate = $gate
                Save-Record $journalPath $envelope
                $process = Start-Process -FilePath $launchExe -ArgumentList $launchArgs `
                    -WorkingDirectory $workflowPath -WindowStyle Hidden -PassThru
                $jobContext = Get-Content -Raw -LiteralPath $contextPath | ConvertFrom-Json -AsHashtable
                $jobContext.process_id = $process.Id
                $jobContext.start_ticks = $process.StartTime.ToUniversalTime().Ticks
                Save-Record $contextPath $jobContext
                $ref = [string]$process.Id
                $result.process_ID = $process.Id
                $envelope.state = 'submitted'
                Save-Record $journalPath $envelope
                Confirm-Result $envelope
                [System.IO.File]::WriteAllText($gate, 'acknowledged', $utf8)
                Remove-Item -LiteralPath $journalPath
                $refs["$($job.submission_index):$($job.jobscript_index)"] = $ref
                Write-Output "Submitted jobscript $($job.submission_index):$($job.jobscript_index): $ref"
                continue
            }
            $stderrPath = $journalPath + '.stderr'
            $exe = $command[0]
            $commandArgs = @($command | Select-Object -Skip 1)
            $stdout = (& $exe @commandArgs 2> $stderrPath) -join "`n"
            $exitCode = $LASTEXITCODE
            $stderr = Get-Content -Raw -LiteralPath $stderrPath
            $envelope.stdout = $stdout
            $envelope.stderr = $stderr
            $envelope.exit_code = $exitCode
            Save-Record $journalPath $envelope
            Remove-Item -LiteralPath $stderrPath
            if ($exitCode -ne 0 -or -not [string]::IsNullOrEmpty($stderr)) {
                throw "Host submission failed (exit $exitCode). Inspect $journalPath."
            }
            $output = $stdout.Trim()
            if ($job.scheduler -eq 'slurm' -and $output -match '^(\d+)(;[^\s;]+)?$') {
                $ref = $Matches[1]
            } elseif ($job.scheduler -eq 'sge' -and $output -match '^(\d+)(\.\d+-\d+:\d+)?$') {
                $ref = $Matches[1]
            } else {
                throw "Cannot parse scheduler job ID. Inspect $journalPath; do not resubmit blindly."
            }
            $result.scheduler_job_ID = $ref
            $envelope.state = 'submitted'
            Save-Record $journalPath $envelope
            Confirm-Result $envelope
            Remove-Item -LiteralPath $journalPath
            $refs["$($job.submission_index):$($job.jobscript_index)"] = $ref
            Write-Output "Submitted jobscript $($job.submission_index):$($job.jobscript_index): $ref"
        }
    } finally {
        Pop-Location
    }
} catch {
    if ($RunJob -and $Context -and $hostContext.ContainsKey('stderr_path')) {
        [System.IO.File]::AppendAllText(
            $hostContext.stderr_path, $_.Exception.Message + "`n", $utf8
        )
    }
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
