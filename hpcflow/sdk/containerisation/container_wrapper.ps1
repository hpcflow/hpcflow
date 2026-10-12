#Requires -Version 7.3

<#
.SYNOPSIS
Run containerised workflows while submitting jobs and executing commands on the host.
.DESCRIPTION
The container prepares jobscripts and records workflow state. This standalone wrapper
validates its JSON plans, launches host commands, and acknowledges successful launches.
Configuration, caches, data and workflow mount records persist on the host.
Display commands can stream raw TTY output; internal JSON exchanges remain separate.
.PARAMETER Image
Container image to run. The install command substitutes the exported default image.
.PARAMETER Machine
hpcflow machine name used for configuration and submission records. Defaults to the
host hostname.
.PARAMETER ContainerCommand
Container executable followed by any fixed arguments. Defaults to docker.
.PARAMETER ConfigArgs
Global hpcflow configuration arguments prepended to each container invocation.
.PARAMETER ResumeResult
Path to a saved submission journal. Retry acknowledgement of a recorded launch,
without submitting that job again; uncertain launch outcomes require reconciliation.
.PARAMETER Context
Path to an internal host-job context containing mount, image and execution settings.
.PARAMETER RunJob
Internal launcher mode: wait for acknowledgement and direct dependencies, then run
the command from Context on the host.
.PARAMETER Tty
Enable raw container terminal output. Defaults to enabled for show and submission
commands. Use -Tty:$false when capturing or redirecting display output.
.PARAMETER NotebookPort
Localhost port for the notebook command. Defaults to 8888; authentication remains
enabled. The container always listens on port 8888 internally.
.PARAMETER HpcflowArgs
hpcflow command and arguments. May be supplied positionally or as an explicit array.
.EXAMPLE
.\hpcflow.ps1 show
Display known submissions with host activity checks and live terminal progress.
.EXAMPLE
.\hpcflow.ps1 go workflow.yaml
Prepare the workflow in the container and submit its jobscripts on the host.
.EXAMPLE
.\hpcflow.ps1 -NotebookPort 8889 notebook
Start JupyterLab for browser or VS Code access using the printed localhost URL.
.EXAMPLE
.\hpcflow.ps1 -Tty:$false -HpcflowArgs @('show', '--json')
Return machine-readable monitoring output without a Docker TTY.
.NOTES
Requires PowerShell 7.3 or newer and a working container CLI. Direct execution
currently requires Windows, PowerShell jobscripts and non-array jobs.
#>

[CmdletBinding(PositionalBinding = $false)]
param(
    [string]$Image = '__APP_NAME_CONTAINER_IMAGE__',
    [string]$Machine = [System.Net.Dns]::GetHostName(),
    [string[]]$ContainerCommand = @('docker'),
    [string[]]$ConfigArgs = @(),
    [string]$ResumeResult,
    [string]$Context,
    [switch]$RunJob,
    [switch]$Tty,
    [ValidateRange(1, 65535)]
    [int]$NotebookPort = 8888,
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$HpcflowArgs = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandArgumentPassing = 'Standard'
$PSNativeCommandUseErrorActionPreference = $false

# /work is the current host directory; registered roots have stable aliases so
# local workflow IDs still resolve when the caller changes directories.
$root = (Get-Location).ProviderPath
$utf8 = [System.Text.UTF8Encoding]::new($false)
$appPrefix = '__APP_NAME_'.Trim('_')
$configEnvName = "${appPrefix}_CONFIG_DIR"
$hostConfigDirectory = $null
$containerConfigDirectory = $null
$showMounts = $false
$monitorMounts = @{}
if (-not $PSBoundParameters.ContainsKey('Tty') -and
    $HpcflowArgs.Count -gt 0 -and $HpcflowArgs[0] -eq 'show') {
    $Tty = $true
}

function Get-MountID {
    <#
    .SYNOPSIS
    Generate a stable identifier for a host workflow mount.
    .PARAMETER Path
    Host root path to hash. Case is normalised on Windows before hashing.
    .OUTPUTS
    System.String. Lower-case hexadecimal SHA-256 mount identifier.
    #>
    param([string]$Path)

    if ($IsWindows) {
        $Path = $Path.ToUpperInvariant()
    }

    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData(
        [Text.Encoding]::UTF8.GetBytes($Path)
    )).ToLowerInvariant()
}

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
    <#
    .SYNOPSIS
    Translate a host path into its container-visible mount path.
    .DESCRIPTION
    Maps shared configuration, the current working root and registered workflow
    roots. Values outside these roots are returned unchanged, including non-path
    command arguments.
    .PARAMETER Value
    Host path or argument to translate.
    .OUTPUTS
    System.String. Mapped container path or the original argument.
    #>
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
    foreach ($mount in $monitorMounts.Keys) {
        $hostRoot = $monitorMounts[$mount]
        if ($Value -eq $hostRoot) { return $mount }
        if ($Value.StartsWith($hostRoot.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar)) {
            return $mount + '/' + $Value.Substring($hostRoot.Length).TrimStart('\', '/').Replace('\', '/')
        }
    }
    return $Value
}

function Receive-ContainerOutput {
    <#
    .SYNOPSIS
    Show startup progress and receive container stdout and stderr.
    .DESCRIPTION
    Shows an interactive spinner until the entrypoint signals container startup
    or either stream first produces output.
    Redirected stderr receives one startup message instead. Raw display preserves
    carriage returns and ANSI cursor controls; protocol output can be captured.
    .PARAMETER Process
    Started process with redirected stdout and stderr. The caller owns disposal.
    .PARAMETER Capture
    Capture stdout as UTF-8 text rather than displaying it.
    .PARAMETER RawDisplay
    Copy stdout bytes to the console instead of returning lines to PowerShell.
    .PARAMETER InputTask
    Optional pending stdin write. Output is drained while it completes, then
    stdin is closed so protocol commands can read their complete payload to EOF.
    .PARAMETER StartedPath
    Host path to the temporary startup signal written by the container entrypoint.
    .OUTPUTS
    System.String. Captured stdout or display lines, unless RawDisplay is enabled.
    Startup progress and container stderr are written directly to stderr.
    #>
    param(
        [Diagnostics.Process]$Process,
        [switch]$Capture,
        [switch]$RawDisplay,
        [Threading.Tasks.Task]$InputTask,
        [string]$StartedPath
    )

    $animate = $Host.UI.SupportsVirtualTerminal -and -not [Console]::IsErrorRedirected
    $frames = @('|', '/', '-', '\')
    $frame = 0
    $stderrBuffer = [byte[]]::new(4096)
    $stderrFirst = $Process.StandardError.BaseStream.ReadAsync(
        $stderrBuffer, 0, $stderrBuffer.Length
    )
    $stdoutBuffer = [byte[]]::new(4096)
    if ($RawDisplay -or $Capture) {
        $stdoutFirst = $Process.StandardOutput.BaseStream.ReadAsync(
            $stdoutBuffer, 0, $stdoutBuffer.Length
        )
    } else {
        $stdoutFirst = $Process.StandardOutput.ReadLineAsync()
    }
    try {
        if ($animate) {
            [Console]::Error.Write("`r$($frames[0]) Starting container...")
        } else {
            [Console]::Error.WriteLine('Starting container...')
        }
        while (-not $stdoutFirst.IsCompleted -and -not $stderrFirst.IsCompleted -and
            -not [IO.File]::Exists($StartedPath)) {
            if ($null -ne $InputTask -and $InputTask.IsCompleted) {
                $null = $InputTask.GetAwaiter().GetResult()
                $Process.StandardInput.Close()
                $InputTask = $null
            }
            Start-Sleep -Milliseconds 100
            if ($animate) {
                $frame++
                if (-not [IO.File]::Exists($StartedPath)) {
                    [Console]::Error.Write("`r$($frames[$frame % $frames.Count]) Starting container...")
                }
            }
        }
    } finally {
        if ($animate) {
            [Console]::Error.Write("`r" + [char]27 + '[2K')
        }
    }

    $stderrStream = [Console]::OpenStandardError()
    $stderrCopy = $null
    # Start stderr copying as soon as its first read completes, even if stdout
    # is silent. Otherwise a full stderr pipe can block container startup.
    while ($true) {
        do {
            if ($null -ne $InputTask -and $InputTask.IsCompleted) {
                $null = $InputTask.GetAwaiter().GetResult()
                $Process.StandardInput.Close()
                $InputTask = $null
            }
            if ($stderrFirst.IsCompleted -and $null -eq $stderrCopy) {
                $count = $stderrFirst.GetAwaiter().GetResult()
                $stderrStream.Write($stderrBuffer, 0, $count)
                $stderrCopy = $Process.StandardError.BaseStream.CopyToAsync($stderrStream)
            }
            if (-not $stdoutFirst.IsCompleted) {
                Start-Sleep -Milliseconds 10
            }
        } while (-not $stdoutFirst.IsCompleted)
        if ($stderrFirst.IsCompleted -and $null -eq $stderrCopy) {
            $count = $stderrFirst.GetAwaiter().GetResult()
            $stderrStream.Write($stderrBuffer, 0, $count)
            $stderrCopy = $Process.StandardError.BaseStream.CopyToAsync($stderrStream)
        }
        if ($RawDisplay -or $Capture) {
            $stdoutStream = if ($Capture) {
                [IO.MemoryStream]::new()
            } else {
                [Console]::OpenStandardOutput()
            }
            $count = $stdoutFirst.GetAwaiter().GetResult()
            $stdoutStream.Write($stdoutBuffer, 0, $count)
            $stdoutCopy = $Process.StandardOutput.BaseStream.CopyToAsync($stdoutStream)
            break
        }
        $line = $stdoutFirst.GetAwaiter().GetResult()
        if ($null -eq $line) {
            break
        }
        Write-Output $line
        $stdoutFirst = $Process.StandardOutput.ReadLineAsync()
    }
    if ($null -eq $stderrCopy) {
        $count = $stderrFirst.GetAwaiter().GetResult()
        $stderrStream.Write($stderrBuffer, 0, $count)
        $stderrCopy = $Process.StandardError.BaseStream.CopyToAsync($stderrStream)
    }
    if ($null -ne $InputTask) {
        $null = $InputTask.GetAwaiter().GetResult()
        $Process.StandardInput.Close()
    }
    $Process.WaitForExit()
    $null = $stderrCopy.GetAwaiter().GetResult()
    if ($RawDisplay -or $Capture) {
        $null = $stdoutCopy.GetAwaiter().GetResult()
        if ($Capture) {
            try {
                return $utf8.GetString($stdoutStream.ToArray())
            } finally {
                $stdoutStream.Dispose()
            }
        }
    }
}

function ConvertTo-WorkflowArguments {
    <#
    .SYNOPSIS
    Move workflow reference options into Click-compatible argument order.
    .PARAMETER CommandArgs
    hpcflow arguments, optionally containing --ref-type or -r after the workflow
    reference. Other command forms are returned unchanged.
    .OUTPUTS
    System.String. Argument words in their normalised order.
    #>
    param([string[]]$CommandArgs)

    # Click requires group options before the workflow reference, even when the
    # wrapper accepts the more natural "workflow ID --ref-type local submit".
    if ($CommandArgs.Count -lt 3 -or $CommandArgs[0] -ne 'workflow') {
        return $CommandArgs
    }

    $refOptions = @()
    $index = 2
    while ($index -lt $CommandArgs.Count) {
        $word = $CommandArgs[$index]

        if ($word -in @('--ref-type', '-r')) {
            if ($index + 1 -ge $CommandArgs.Count) {
                throw "Missing value for $word."
            }

            $refOptions += $CommandArgs[$index..($index + 1)]
            $index += 2
        } elseif ($word.StartsWith('--ref-type=')) {
            $refOptions += $word
            $index++
        } else {
            break
        }
    }

    return @('workflow') + $refOptions + @($CommandArgs[1]) +
        @($CommandArgs | Select-Object -Skip $index)
}

function Invoke-Container {
    <#
    .SYNOPSIS
    Invoke hpcflow in the container with shared mounts and host-aware settings.
    .DESCRIPTION
    Resolves persistent configuration and registered workflow roots, translates
    arguments, and selects JSON capture or terminal display. Restores console
    encoding and removes temporary exchange files on success or failure.
    The native exit status is saved in script-scoped containerExitCode.
    .PARAMETER CommandArgs
    hpcflow command arguments, excluding the executable.
    .PARAMETER InputJson
    JSON payload to send through stdin, or a shared snapshot file when using a TTY.
    .PARAMETER Capture
    Return captured stdout as a single string and fail on a nonzero exit status.
    .PARAMETER DisplayOutput
    Allow colour and, when enabled by Tty, raw terminal display.
    .PARAMETER SubmissionPlan
    Capture a submission plan separately from TTY progress using a temporary file.
    Requires Capture and DisplayOutput for the TTY preparation path.
    .PARAMETER Notebook
    Start JupyterLab instead of the hpcflow entrypoint. Publish its port only on
    host loopback, retain token authentication and reuse the normal mounts.
    .OUTPUTS
    System.String. Captured output or submission-plan JSON when requested.
    Otherwise output is passed through or written directly to the console.
    #>
    param(
        [string[]]$CommandArgs,
        [string]$InputJson,
        [switch]$Capture,
        [switch]$DisplayOutput,
        [switch]$SubmissionPlan,
        [switch]$Notebook
    )

    $CommandArgs = @(ConvertTo-WorkflowArguments $CommandArgs)

    $useTty = $Tty -and $DisplayOutput -and (-not $Capture -or $SubmissionPlan)
    $hostEnv = @('--env', "TTY_INTERACTIVE=$([int][bool]$useTty)")
    if ($useTty) {
        $size = $Host.UI.RawUI.WindowSize
        if ($size.Width -gt 0) { $hostEnv += @('--env', "COLUMNS=$($size.Width)") }
        if ($size.Height -gt 0) { $hostEnv += @('--env', "LINES=$($size.Height)") }
        $hostEnv += @('--env', 'TERM=xterm-256color')
    }
    $useColour = $DisplayOutput -and (-not $Capture -or $useTty) -and -not $env:NO_COLOR -and (
        $PSStyle.OutputRendering -eq 'ANSI' -or (
            $PSStyle.OutputRendering -eq 'Host' -and
            $Host.UI.SupportsVirtualTerminal -and -not [Console]::IsOutputRedirected
        )
    )
    if ($useColour) {
        $hostEnv += @('--env', 'FORCE_COLOR=1', '--env', 'NO_COLOR=')
    } elseif ($useTty) {
        $hostEnv += @('--env', 'FORCE_COLOR=1', '--env', 'NO_COLOR=1')
    } else {
        $hostEnv += @('--env', 'FORCE_COLOR=', '--env', 'NO_COLOR=1')
    }
    # Share configuration, cache and data with successive container invocations,
    # but keep this storage separate from the native application's directory.
    $configMount = @()
    $configPath = [System.Environment]::GetEnvironmentVariable($configEnvName)
    if ($IsWindows -and $configPath -and
        ($configPath -eq '/work' -or $configPath.StartsWith('/work/')) -and
        -not $hostConfigDirectory) {
        # Preserve an explicitly container-visible configuration path.
        $script:hostConfigDirectory = Get-HostPath $configPath
        [IO.Directory]::CreateDirectory($hostConfigDirectory) | Out-Null
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
    $mountID = Get-MountID $root
    if ($showMounts -and -not $hostConfigDirectory) {
            throw 'Host show requires the shared host configuration directory, not a /work configuration override.'
    }
    if ($hostConfigDirectory) {
        $mountDirectory = Join-Path $hostConfigDirectory 'host-mounts'
        if (Test-Path -LiteralPath $mountDirectory) {
            foreach ($file in Get-ChildItem -LiteralPath $mountDirectory -Filter '*.json') {
                $record = Get-Content -Raw -LiteralPath $file.FullName | ConvertFrom-Json -AsHashtable
                if ($record.schema_version -ne 1 -or $record.mount_root -isnot [string] -or
                    -not [IO.Path]::IsPathFullyQualified($record.mount_root) -or
                    $file.BaseName -ne (Get-MountID $record.mount_root)) {
                    throw "Invalid host mount record: $($file.FullName)"
                }
                $target = "/host-workflows/$($file.BaseName)"
                $script:monitorMounts[$target] = $record.mount_root
                if (Test-Path -LiteralPath $record.mount_root -PathType Container) {
                    $configMount += @('--mount', "type=bind,source=$($record.mount_root),target=$target")
                }
            }
        }
    }
    $hostEnv += @(
        '--env', "${configEnvName}=$configPath",
        '--env', "${appPrefix}_CONTAINER_WORK_MOUNT=/host-workflows/$mountID",
        '--env', "${appPrefix}_CONTAINER_HOST_CONFIG_DIR=$hostConfigDirectory",
        '--env', "${appPrefix}_CONTAINER_HOST_WORK_DIR=$root",
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
    if ($Notebook) {
        # Notebook kernels use the container's Python and shared application
        # storage. Do not forward hpcflow CLI options to the Jupyter server.
        $mappedArgs = @(
            'python',
            '-m', 'jupyterlab',
            '--ServerApp.ip=0.0.0.0',
            '--ServerApp.port=8888',
            '--ServerApp.port_retries=0',
            '--ServerApp.open_browser=False',
            '--ServerApp.allow_root=True',
            '--ServerApp.root_dir=/work',
            '--ServerApp.custom_display_url', "http://127.0.0.1:$NotebookPort"
        )
    } else {
        $mappedArgs = @($ConfigArgs + $CommandArgs | ForEach-Object {
            ConvertTo-ContainerPath $_
        })
        $mappedArgs = @('--with-config', 'machine', $Machine) + $mappedArgs
    }
    $runArgs = @('run', '--rm')
    if ($Notebook) {
        $runArgs += @(
            '--publish', "127.0.0.1:${NotebookPort}:8888",
            '--entrypoint', 'hpcflow-container-entrypoint'
        )
    }
    if ($useTty) {
        $runArgs += '-t'
    }
    if (-not $useTty -or (
        -not $PSBoundParameters.ContainsKey('InputJson') -and
        -not [Console]::IsInputRedirected
    )) {
        $runArgs += '-i'
    }
    $dockerArgs = $runArgs + @(
        '--mount', "type=bind,source=$root,target=/work",
        '--workdir', '/work'
    ) + $configMount + $hostEnv + @($Image) + $mappedArgs
    $exe = $ContainerCommand[0]
    $prefix = @($ContainerCommand | Select-Object -Skip 1)
    $previousConsoleEncoding = [Console]::OutputEncoding
    $OutputEncoding = $utf8
    $snapshotPath = $null
    $planPath = $null
    $startedName = ".host-started-$([Guid]::NewGuid().ToString('N'))"
    $startedPath = Join-Path $hostConfigDirectory $startedName
    $imageIndex = $dockerArgs.Count - $mappedArgs.Count - 1
    $dockerArgs = @($dockerArgs | Select-Object -First $imageIndex) +
        @('--env', "HPCFLOW_CONTAINER_STARTED_FILE=$containerConfigDirectory/$startedName") +
        @($dockerArgs | Select-Object -Skip $imageIndex)
    $process = $null
    try {
        # PowerShell otherwise decodes Docker's UTF-8 output using the caller's
        # console code page. The finally block restores the caller's setting.
        [Console]::OutputEncoding = $utf8
        if ($useTty) {
            # A TTY mixes progress with stdout, so exchange machine-readable
            # plans and monitoring snapshots through the shared config mount.
            if ($SubmissionPlan) {
                $planName = ".host-plan-$([Guid]::NewGuid().ToString('N')).json"
                $planPath = Join-Path $hostConfigDirectory $planName
                $imageIndex = $dockerArgs.Count - $mappedArgs.Count - 1
                $dockerArgs = @($dockerArgs | Select-Object -First $imageIndex) +
                    @('--env', "${appPrefix}_CONTAINER_PLAN_FILE=$containerConfigDirectory/$planName") +
                    @($dockerArgs | Select-Object -Skip $imageIndex)
            }
            if ($PSBoundParameters.ContainsKey('InputJson')) {
                $snapshotName = ".host-show-$([Guid]::NewGuid().ToString('N')).json"
                $snapshotPath = Join-Path $hostConfigDirectory $snapshotName
                [IO.File]::WriteAllText($snapshotPath, $InputJson, $utf8)
                $dockerArgs += @('--snapshot-file', "$containerConfigDirectory/$snapshotName")
            }
        }
        $startInfo = [Diagnostics.ProcessStartInfo]::new()
        $startInfo.FileName = $exe
        $startInfo.WorkingDirectory = $root
        $startInfo.UseShellExecute = $false
        $startInfo.RedirectStandardOutput = $true
        $startInfo.RedirectStandardError = $true
        $startInfo.StandardOutputEncoding = $utf8
        $startInfo.StandardErrorEncoding = $utf8
        $sendJson = $PSBoundParameters.ContainsKey('InputJson') -and -not $useTty
        $startInfo.RedirectStandardInput = $sendJson
        if ($sendJson) {
            $startInfo.StandardInputEncoding = $utf8
        }
        foreach ($arg in @($prefix) + $dockerArgs) {
            $startInfo.ArgumentList.Add($arg)
        }
        $process = [Diagnostics.Process]::Start($startInfo)
        $inputTask = $null
        if ($sendJson) {
            $process.StandardInput.AutoFlush = $true
            $inputTask = $process.StandardInput.WriteLineAsync($InputJson)
        }
        $captureOutput = ($Capture -or $sendJson) -and -not $useTty -and -not $useColour
        if ($captureOutput) {
            $out = Receive-ContainerOutput $process -Capture -InputTask $inputTask -StartedPath $startedPath
        } else {
            Receive-ContainerOutput $process -RawDisplay:($useTty -or $useColour -or $Notebook) -InputTask $inputTask -StartedPath $startedPath
        }
        $script:containerExitCode = $process.ExitCode
        if (($Capture -or $PSBoundParameters.ContainsKey('InputJson')) -and $process.ExitCode -ne 0) {
            throw "Container command failed with exit code $($process.ExitCode)."
        }
        if ($useTty -and $SubmissionPlan) {
            if (-not (Test-Path -LiteralPath $planPath -PathType Leaf)) {
                throw 'Container did not write a host submission plan.'
            }
            return [IO.File]::ReadAllText($planPath, $utf8)
        }
        if ($captureOutput) {
            return $out.TrimEnd("`r", "`n")
        }
    } finally {
        [Console]::OutputEncoding = $previousConsoleEncoding
        if ($process) {
            if (-not $process.HasExited) {
                $process.Kill($true)
                $process.WaitForExit()
            }
            $process.Dispose()
        }
        if ($snapshotPath -and (Test-Path -LiteralPath $snapshotPath)) {
            Remove-Item -LiteralPath $snapshotPath
        }
        if ($planPath -and (Test-Path -LiteralPath $planPath)) {
            Remove-Item -LiteralPath $planPath
        }
        if (Test-Path -LiteralPath $startedPath) {
            Remove-Item -LiteralPath $startedPath
        }
    }
}

function Get-HostPath {
    <#
    .SYNOPSIS
    Resolve a container mount path to an absolute host path.
    .DESCRIPTION
    Accepts /work and registered workflow aliases. Rejects paths outside recognised
    mounts, backslashes in the relative path, and parent-directory traversal.
    .PARAMETER ContainerPath
    Slash-separated container path to resolve.
    .PARAMETER AllowConfig
    Also permit paths within the shared container configuration mount.
    .OUTPUTS
    System.String. Absolute path in the corresponding host mount.
    #>
    param(
        [string]$ContainerPath,
        [switch]$AllowConfig
    )

    $hostRoot = $root
    $containerRoot = '/work'
    foreach ($mount in $monitorMounts.Keys) {
        if ($ContainerPath -eq $mount -or $ContainerPath.StartsWith($mount + '/')) {
            $hostRoot = $monitorMounts[$mount]
            $containerRoot = $mount
            break
        }
    }
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
    <#
    .SYNOPSIS
    Persist a JSON journal or context with a flushed file replacement.
    .PARAMETER Path
    Destination record path. Its parent directory must already exist.
    .PARAMETER Record
    Object to serialise as UTF-8 JSON with a trailing LF newline.
    .OUTPUTS
    None. Writes a temporary sibling file and replaces the destination.
    #>
    param(
        [string]$Path,
        [object]$Record
    )

    # Flush the replacement before moving it: recovery must see either the old
    # durable record or the complete new one, never a partially written result.
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
    <#
    .SYNOPSIS
    Acknowledge a successful host launch and persist its workflow mount identity.
    .DESCRIPTION
    Sends the saved result to the container and validates the acknowledgement.
    Failure propagates to the caller, which retains the launch journal for recovery
    rather than launching the job again.
    .PARAMETER Envelope
    Host submission envelope containing result and container workflow_path.
    .OUTPUTS
    None. Updates container metadata and the persistent host mount record.
    #>
    param([hashtable]$Envelope)

    # A successful host launch is not retried if acknowledgement fails. Its
    # journal survives so ResumeResult can repeat only the metadata update.
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
    $mountDirectory = Join-Path $hostConfigDirectory 'host-mounts'
    [IO.Directory]::CreateDirectory($mountDirectory) | Out-Null
    Save-Record (Join-Path $mountDirectory ((Get-MountID $root) + '.json')) @{
        schema_version = 1
        mount_root = $root
    }
}

function Invoke-HostShow {
    <#
    .SYNOPSIS
    Collect host activity and render container-managed submission metadata.
    .DESCRIPTION
    Requests a monitoring plan, checks direct process identities on the host and
    runs each distinct scheduler query once. Sends the resulting snapshot back
    to the container for validation and rendering. Query failures are errors,
    not evidence that jobs are inactive.
    .PARAMETER ShowArgs
    Arguments forwarded to the internal display or known-submissions command.
    .PARAMETER GetKnown
    Render known-submission records instead of the show table.
    .OUTPUTS
    System.String. Rendered output when returned by the container invocation.
    TTY and coloured display may write directly to the console instead.
    #>
    param(
        [string[]]$ShowArgs,
        [switch]$GetKnown
    )

    $script:showMounts = $true
    $plan = Invoke-Container -CommandArgs @('internal', 'prepare-host-show') -Capture |
        ConvertFrom-Json -AsHashtable
    if ($plan.schema_version -ne 1 -or $plan.jobs -isnot [array]) {
        throw 'Invalid host monitoring plan.'
    }
    $results = @()
    $queries = @{}
    foreach ($job in $plan.jobs) {
        $result = @{
            workflow_path = $job.workflow_path
            workflow_id = $job.workflow_id
            submission_index = $job.submission_index
            jobscript_index = $job.jobscript_index
        }
        if ($job.scheduler -eq 'direct') {
            $mount = @($monitorMounts.Keys | Where-Object {
                $job.workflow_path.StartsWith($_ + '/')
            })
            if ($mount.Count -ne 1) { throw 'Unknown host workflow mount.' }
            $relative = $job.workflow_path.Substring($mount[0].Length).TrimStart('/')
            if ($relative.Contains('\') -or @($relative.Split('/')) -contains '..') {
                throw 'Invalid monitored workflow path.'
            }
            $workflowHostPath = Join-Path $monitorMounts[$mount[0]] $relative
            $proc = Get-Process -Id $job.process_id -ErrorAction SilentlyContinue
            $result.active = $false
            if ($null -ne $proc) {
                $contextPath = Join-Path $workflowHostPath (
                    ".${appPrefix}-host-$($job.submission_index)-$($job.jobscript_index).json"
                )
                if (-not (Test-Path -LiteralPath $contextPath)) {
                    throw "Cannot verify host process identity: $contextPath"
                }
                $record = Get-Content -Raw -LiteralPath $contextPath | ConvertFrom-Json -AsHashtable
                if ($record.workflow_id -ne $job.workflow_id -or
                    $record.process_id -ne $job.process_id) {
                    throw 'Conflicting host process monitoring context.'
                }
                $result.active = $proc.StartTime.ToUniversalTime().Ticks -eq $record.start_ticks
            }
        } elseif ($job.scheduler -in @('slurm', 'sge')) {
            if ($job.query -isnot [array] -or -not $job.query.Count) {
                throw 'Invalid scheduler monitoring command.'
            }
            $key = ConvertTo-Json -InputObject $job.query -Compress
            if (-not $queries.ContainsKey($key)) {
                $exe = $job.query[0]
                $queryArgs = @($job.query | Select-Object -Skip 1 | ForEach-Object {
                    if ($_ -eq '$USER') { [Environment]::UserName } else { $_ }
                })
                $stderrPath = [IO.Path]::GetTempFileName()
                try {
                    $stdout = (& $exe @queryArgs 2> $stderrPath) -join "`n"
                    $code = $LASTEXITCODE
                    $stderr = [IO.File]::ReadAllText($stderrPath)
                    if ($code -ne 0 -or -not [string]::IsNullOrWhiteSpace($stderr)) {
                        throw "Host monitoring failed (exit $code): $stderr"
                    }
                    $queries[$key] = $stdout
                } finally {
                    Remove-Item -LiteralPath $stderrPath
                }
            }
            $result.stdout = $queries[$key]
        } else {
            throw "Unsupported host monitoring scheduler: $($job.scheduler)"
        }
        $results += $result
    }
    $snapshot = ConvertTo-Json -InputObject @{
        schema_version = 1
        jobs = $results
    } -Depth 30 -Compress
    $command = if ($GetKnown) { 'get-known-host' } else { 'show-host' }
    $output = Invoke-Container -CommandArgs (@('internal', $command) + $ShowArgs) `
        -InputJson $snapshot -DisplayOutput:($ShowArgs -notcontains '--json')
    if ($output) { Write-Output $output }
}

function Get-DirectDependency {
    <#
    .SYNOPSIS
    Identify a still-running direct dependency without confusing a reused PID.
    .PARAMETER Dependency
    Dependency record containing submission_index and jobscript_index.
    .PARAMETER Reference
    Host process ID recorded for the dependency.
    .PARAMETER WorkflowHostPath
    Host workflow directory containing the launcher's saved context.
    .OUTPUTS
    System.Collections.Hashtable. process_id and start_ticks for a matching live
    launcher. No output if the process exited or its identity no longer matches.
    A live process without a saved context cannot be verified and causes an error.
    #>
    param(
        [hashtable]$Dependency,
        [string]$Reference,
        [string]$WorkflowHostPath
    )

    # A PID alone is not an identity: Windows may reuse it after a job exits.
    $proc = Get-Process -Id ([int]$Reference) -ErrorAction SilentlyContinue
    if ($null -eq $proc) {
        return
    }

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
    <#
    .SYNOPSIS
    Prepare a run in the container, execute it on the host, and record completion.
    .DESCRIPTION
    Validates the run plan, maps host paths and temporarily applies its environment.
    Journals execution before acknowledging the exit code in the container, restores
    the host environment, and refuses to relaunch a run with an unresolved journal.
    Command-free runs already finalised by the container require no host execution.
    .PARAMETER RunArgs
    Internal workflow execute-run arguments, including the workflow reference and
    element, action and run indices used to identify the completion request.
    .OUTPUTS
    Host command output. Completion is recorded separately through the JSON protocol.
    #>
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
        if ($_ -eq '/work' -or $_.StartsWith('/work/') -or $_.StartsWith('/host-workflows/') -or
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
            if ($value -eq '/work' -or $value.StartsWith('/work/') -or $value.StartsWith('/host-workflows/') -or
                ($hostConfigDirectory -and ($value -eq $containerConfigDirectory -or
                 $value.StartsWith($containerConfigDirectory + '/')))) {
                $value = Get-HostPath $value -AllowConfig
            }
            [Environment]::SetEnvironmentVariable($key, $value)
        }
        $configEnvName = "${appPrefix}_CONFIG_DIR"
        if (-not $previous.ContainsKey($configEnvName)) {
            $previous[$configEnvName] = [Environment]::GetEnvironmentVariable($configEnvName)
        }
        [Environment]::SetEnvironmentVariable($configEnvName, $hostConfigDirectory)
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
    if ([string]::IsNullOrWhiteSpace($Machine)) {
        throw 'Machine must not be empty; omit it to use the host hostname.'
    }
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
    if ($HpcflowArgs[0] -eq 'notebook') {
        if ($HpcflowArgs.Count -ne 1 -or $ConfigArgs.Count -or $Context) {
            throw 'Use notebook without hpcflow arguments, ConfigArgs or Context; select the host port with NotebookPort.'
        }
        Write-Host "Starting JupyterLab at http://127.0.0.1:$NotebookPort; use the token URL printed below."
        Write-Host 'Open the URL in a browser, or select an existing Jupyter server in VS Code. Press Ctrl+C to stop.'
        Invoke-Container -CommandArgs @() -Notebook
        exit $script:containerExitCode
    }
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
    if ($HpcflowArgs[0] -eq 'show' -and $HpcflowArgs -notcontains '--help' -and
        $HpcflowArgs -notcontains '--legend') {
        Invoke-HostShow @($HpcflowArgs | Select-Object -Skip 1)
        exit 0
    }
    if ($HpcflowArgs.Count -ge 2 -and $HpcflowArgs[0] -eq 'submission' -and
        $HpcflowArgs[1] -eq 'get-known' -and $HpcflowArgs -notcontains '--help') {
        Invoke-HostShow -ShowArgs @($HpcflowArgs | Select-Object -Skip 2) -GetKnown
        exit 0
    }
    $workflowCommandIndex = 2
    if ($HpcflowArgs[0] -eq 'workflow') {
        $refOptions = @()
        $index = 1
        while ($index -lt $HpcflowArgs.Count) {
            $word = $HpcflowArgs[$index]
            if ($word -in @('--ref-type', '-r')) {
                if ($index + 1 -ge $HpcflowArgs.Count) { throw "Missing value for $word." }
                $refOptions += $HpcflowArgs[$index..($index + 1)]
                $index += 2
            } elseif ($word.StartsWith('--ref-type=')) {
                $refOptions += $word
                $index++
            } else { break }
        }
        if ($refOptions.Count) {
            if ($index -ge $HpcflowArgs.Count) { throw 'Missing workflow reference.' }
            $HpcflowArgs = @('workflow', $HpcflowArgs[$index]) + $refOptions +
                @($HpcflowArgs | Select-Object -Skip ($index + 1))
        }
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
    if ($submission -and -not $PSBoundParameters.ContainsKey('Tty')) {
        $Tty = $true
    }
    if (-not $submission -and (
        ($HpcflowArgs[0] -eq 'workflow' -and $HpcflowArgs -contains 'submit') -or
        ($HpcflowArgs[0] -eq 'demo-workflow' -and $HpcflowArgs -contains 'go')
    )) {
        throw 'Unsupported submission argument order; use the documented command-first invocation.'
    }
    if (-not $submission) {
        if ($HpcflowArgs[0] -eq 'workflow' -and
            $workflowCommandIndex -lt $HpcflowArgs.Count -and
            $HpcflowArgs[$workflowCommandIndex] -eq 'abort-run') {
            throw 'Aborting container host runs is not supported yet.'
        }
        if ($IsWindows -and $HpcflowArgs[0] -eq 'workflow' -and
            $workflowCommandIndex -lt $HpcflowArgs.Count -and
            $HpcflowArgs[$workflowCommandIndex] -in @('wait', 'cancel')) {
            throw 'Host process monitoring and cancellation are not yet supported by this wrapper.'
        }
        Invoke-Container -CommandArgs $HpcflowArgs -DisplayOutput:($HpcflowArgs -notcontains '--json')
        exit $script:containerExitCode
    }
    if ([string]::IsNullOrWhiteSpace($Machine)) {
        throw 'Machine must not be empty; omit it to use the host hostname.'
    }
    if ($HpcflowArgs -contains '--wait' -or $HpcflowArgs -contains '--cancel') {
        throw 'Containerised submission cannot wait for or cancel jobs.'
    }
    $prepareArgs = @($HpcflowArgs)
    if ($HpcflowArgs -notcontains '--containerised') { $prepareArgs += '--containerised' }
    $plan = Invoke-Container -CommandArgs $prepareArgs -Capture -SubmissionPlan -DisplayOutput |
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
        ) {
            throw 'Invalid jobscript indices.'
        }

        $key = "$($job.submission_index):$($job.jobscript_index)"
        if ($seen.ContainsKey($key)) {
            throw "Duplicate jobscript: $key"
        }

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
        if ($job.dependencies -isnot [array]) {
            throw 'Invalid dependencies list.'
        }

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
            ) {
                throw "Unresolved or out-of-order dependency: $depKey"
            }

            if ($null -ne $dep.reference -and (
                $dep.reference -isnot [string] -or $dep.reference -notmatch '^\d+$'
            )) {
                throw "Invalid dependency reference: $depKey"
            }

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
                    if ($null -eq $ref) {
                        $ref = $refs["$($dep.submission_index):$($dep.jobscript_index)"]
                    }

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
                # The launcher waits on an acknowledgement gate before running,
                # so workflow metadata always knows its PID before commands start.
                $dependencies = @(
                    foreach ($dep in $job.dependencies) {
                        $pidValue = $dep.reference
                        if ($null -eq $pidValue) {
                            $pidValue = $refs["$($dep.submission_index):$($dep.jobscript_index)"]
                        }

                        Get-DirectDependency $dep ([string]$pidValue) $workflowPath
                    }
                )
                $prefix = ".${appPrefix}-host-$($job.submission_index)-$($job.jobscript_index)"
                $contextPath = Join-Path $workflowPath ($prefix + '.json')
                $gate = Join-Path $workflowPath ($prefix + '.ack')
                if (Test-Path -LiteralPath $gate) {
                    throw "Acknowledgement gate already exists: $gate"
                }

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

            # Queued schedulers return a job ID; preserve their full output in
            # the journal before parsing it or acknowledging the launch.
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
