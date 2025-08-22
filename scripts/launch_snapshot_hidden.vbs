' scripts/launch_snapshot_hidden.vbs
Dim shell, cmd
Set shell = CreateObject("WScript.Shell")
cmd = "powershell -NoProfile -ExecutionPolicy Bypass -File ""C:\Users\sfsch\Documents\fpl_project\scripts\run_snapshot.ps1"""
shell.Run cmd, 0, True
