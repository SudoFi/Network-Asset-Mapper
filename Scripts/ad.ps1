# Fetches user data from Active Directory and exports it to a CSV.

# Define output path for the CSV in the parent's 'Import' folder.
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$OutputPath = Join-Path -Path (Join-Path -Path $ScriptDir -ChildPath "..") -ChildPath "Import\user_ad_data.csv"

try {
    Write-Host "--- Active Directory User Pull ---" -ForegroundColor Yellow
    
    # Prompt for AD credentials.
    $credential = Get-Credential
    if (-not $credential) {
        Write-Warning "Credential prompt cancelled."
        Read-Host -Prompt "Press Enter to return to the main script"
        exit
    }

    # Define attributes to retrieve for each user.
    $properties = @(
        'DisplayName', 'GivenName', 'Surname', 'EmailAddress', 
        'Title', 'Department', 'Manager'
    )

    Write-Host "Querying Active Directory for all enabled users..."
    
    # Query AD for all enabled users and their properties.
    $users = Get-ADUser -Filter 'Enabled -eq $true' -Properties * -Credential $credential | Select-Object -Property $properties
    
    if ($users) {
        Write-Host "Successfully retrieved $($users.Count) users." -ForegroundColor Green
        Write-Host "Exporting to CSV at: $OutputPath"
        
        # Remap AD attributes to CSV headers and parse manager's name.
        $exportData = $users | Select-Object @{Name="User Display Name"; Expression={$_.DisplayName}},
                                           @{Name="User First Name"; Expression={$_.GivenName}},
                                           @{Name="User Last Name"; Expression={$_.Surname}},
                                           @{Name="User Email"; Expression={$_.EmailAddress}},
                                           @{Name="User Title"; Expression={$_.Title}},
                                           @{Name="User Department"; Expression={$_.Department}},
                                           @{Name="User Manager"; Expression={ if ($_.Manager) { ($_.Manager -split ',')[0] -replace 'CN=' } else { $null } }}

        # Export the processed data to the CSV file.
        $exportData | Export-Csv -Path $OutputPath -NoTypeInformation -Force
        
        Write-Host "Export complete." -ForegroundColor Green
    }
    else {
        Write-Warning "No users found or query failed."
    }
}
catch {
    # Catch and display any errors.
    Write-Error "An error occurred during the Active Directory query: $_"
}

# Pause to display final status.
Read-Host -Prompt "Press Enter to return to the main script"