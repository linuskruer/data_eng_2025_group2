# PowerShell script to run ClickHouse analytical queries via Docker
# Usage: .\run_queries_docker.ps1

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$queriesDir = $scriptDir
$outputDir = Join-Path $scriptDir "results"

Write-Host "================================================================================="
Write-Host "ClickHouse Analytical Queries Runner (via Docker)"
Write-Host "================================================================================="

# Get all SQL files
$sqlFiles = Get-ChildItem -Path $queriesDir -Filter "*.sql" | Where-Object { $_.Name -ne "*.sql" }

Write-Host "`nFound $($sqlFiles.Count) SQL query files:"
$sqlFiles | ForEach-Object { Write-Host "  - $($_.Name)" }

# Create results directory
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$successful = 0
$failed = 0

foreach ($sqlFile in $sqlFiles) {
    $queryName = $sqlFile.BaseName
    Write-Host "`n================================================================"
    Write-Host "Running: $queryName"
    Write-Host "================================================================"
    
    try {
        # Read query
        $query = Get-Content $sqlFile.FullName -Raw
        
        # Execute via Docker with column names in output
        $result = docker exec clickhouse-server clickhouse-client --password mypassword --multiquery --format TSVWithNames --query $query 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "SUCCESS - Query executed"
            
            # Parse tab-separated results with column names
            $lines = $result -split "`r?`n" | Where-Object { $_.Trim() -ne "" }
            
            if ($lines.Count -gt 1) {
                # First line contains column names (TSVWithNames format)
                $columnNames = ($lines[0] -split "`t") | ForEach-Object { $_.Trim() }
                $dataRows = @()
                
                # Parse data rows (skip first line which is headers)
                for ($i = 1; $i -lt $lines.Count; $i++) {
                    $line = $lines[$i]
                    if ($line.Trim() -ne "") {
                        $values = $line -split "`t"
                        $cleanValues = @()
                        foreach ($val in $values) {
                            $cleanValues += $val.Trim()
                        }
                        if ($cleanValues.Count -gt 0 -and $cleanValues.Count -eq $columnNames.Count) {
                            $dataRows += ,@($cleanValues)
                        }
                    }
                }
                
                # Build structured results with proper row objects
                $structuredData = @()
                foreach ($row in $dataRows) {
                    $rowObj = @{}
                    for ($i = 0; $i -lt [Math]::Min($columnNames.Count, $row.Count); $i++) {
                        $rowObj[$columnNames[$i]] = $row[$i]
                    }
                    $structuredData += $rowObj
                }
                
                $structuredResults = @{
                    query_name = $queryName
                    execution_time = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
                    status = "success"
                    row_count = $dataRows.Count
                    columns = $columnNames
                    data = $structuredData
                }
                
                # Save as JSON
                $outputFile = Join-Path $outputDir "$queryName.json"
                $structuredResults | ConvertTo-Json -Depth 10 | Set-Content $outputFile -Encoding UTF8
                
                Write-Host "   Rows returned: $($dataRows.Count)"
                Write-Host "   Results saved to: $outputFile"
                if ($dataRows.Count -gt 0) {
                    Write-Host "   Sample (first row): $($dataRows[0] -join ' | ')"
                }
            } else {
                Write-Host "   ⚠️  Query returned no rows"
                $outputFile = Join-Path $outputDir "$queryName.json"
                @{
                    query_name = $queryName
                    execution_time = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
                    status = "success"
                    row_count = 0
                    columns = @()
                    data = @()
                } | ConvertTo-Json | Set-Content $outputFile -Encoding UTF8
            }
            
            $successful++
        } else {
            Write-Host "FAILED - Error: $result"
            $failed++
        }
    } catch {
        Write-Host "ERROR: $_"
        $failed++
    }
}

Write-Host "`n================================================================"
Write-Host "SUMMARY"
Write-Host "================================================================"
Write-Host "Total queries: $($sqlFiles.Count)"
Write-Host "Successful: $successful"
Write-Host "Failed: $failed"
Write-Host "Results saved to: $outputDir"
Write-Host "================================================================`n"


