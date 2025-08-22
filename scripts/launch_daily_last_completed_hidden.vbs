' scripts/launch_daily_last_completed_hidden.vbs
Dim shell, cmd
Set shell = CreateObject("WScript.Shell")
cmd = "powershell -NoProfile -ExecutionPolicy Bypass -File ""C:\Users\sfsch\Documents\fpl_project\scripts\run_daily_last_completed.ps1"""
shell.Run cmd, 0, True
