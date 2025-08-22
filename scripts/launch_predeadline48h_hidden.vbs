' scripts/launch_predeadline48h_hidden.vbs
Dim shell, cmd
Set shell = CreateObject("WScript.Shell")
cmd = "powershell -NoProfile -ExecutionPolicy Bypass -File ""C:\Users\sfsch\Documents\fpl_project\scripts\run_predeadline48h.ps1"""
shell.Run cmd, 0, True
