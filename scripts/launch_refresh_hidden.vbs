' scripts/launch_refresh_hidden.vbs
Dim shell, cmd
Set shell = CreateObject("WScript.Shell")
cmd = "powershell -NoProfile -ExecutionPolicy Bypass -File ""C:\Users\sfsch\Documents\fpl_project\scripts\run_refresh_pre_deadlines.ps1"""
' 0 = caché, True = attendre la fin
shell.Run cmd, 0, True
