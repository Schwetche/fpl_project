Set WshShell = CreateObject("Wscript.Shell")
WshShell.Run "powershell.exe -NoLogo -ExecutionPolicy Bypass -File ""C:\Users\sfsch\Documents\fpl_project\scripts\run_snapshot.ps1""", 0, True
