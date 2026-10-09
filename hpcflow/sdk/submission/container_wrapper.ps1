#Requires -Version 7.3
[CmdletBinding()]
param(
    [string]$Image = '__APP_NAME_CONTAINER_IMAGE__',
    [string]$Machine,
    [string[]]$ContainerCommand = @('docker'),
    [string[]]$ConfigArgs = @(),
    [string]$ResumeResult,
    [string[]]$HpcflowArgs = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandArgumentPassing = 'Standard'
$PSNativeCommandUseErrorActionPreference = $false
$root = (Get-Location).ProviderPath
$utf8 = [System.Text.UTF8Encoding]::new($false)

function Invoke-Container {
    param([string[]]$CommandArgs, [string]$InputJson, [switch]$Capture)
    $dockerArgs = @(
        'run', '--rm', '-i', '--mount', "type=bind,source=$root,target=/work",
        '--workdir', '/work', $Image
    ) + $ConfigArgs + $CommandArgs
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
    param([string]$ContainerPath)
    if ($ContainerPath -ne '/work' -and -not $ContainerPath.StartsWith('/work/')) {
        throw "Container path is outside /work: $ContainerPath"
    }
    $relative = $ContainerPath.Substring(5).TrimStart('/')
    if ($relative.Contains('\') -or @($relative.Split('/')) -contains '..') {
        throw "Invalid container path: $ContainerPath"
    }
    return [System.IO.Path]::GetFullPath((Join-Path $root $relative))
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

try {
    if (
        -not $ContainerCommand.Count -or [string]::IsNullOrWhiteSpace($Image) -or
        $Image -match '^__APP_NAME_.*__$'
    ) {
        throw 'Image and ContainerCommand must not be empty.'
    }
    if ($root.Contains(',')) { throw 'The mounted directory must not contain a comma.' }
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
        Remove-Item -LiteralPath $ResumeResult
        Write-Output 'Host submission acknowledged; no job was launched.'
        exit 0
    }
    if (-not $HpcflowArgs.Count) { throw 'Supply HpcflowArgs or ResumeResult.' }
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
        Invoke-Container -CommandArgs $HpcflowArgs
        exit $script:containerExitCode
    }
    if ([string]::IsNullOrWhiteSpace($Machine)) {
        throw 'Supply Machine using the host hpcflow machine configuration name.'
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
        if ($job.scheduler -notin @('slurm', 'sge')) {
            throw "Unsupported scheduler: $($job.scheduler)"
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
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
