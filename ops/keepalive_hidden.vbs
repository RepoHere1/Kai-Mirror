' KAI KEEPALIVE HIDDEN LAUNCHER (2026-11-09) - kills the 2-minute PowerShell popup.
' The scheduled task used to run powershell.exe directly, which flashes a console
' window on the desktop every 2 minutes. wscript.exe never owns a console, and
' the PowerShell it spawns runs with window style 0 (fully hidden).
CreateObject("WScript.Shell").Run _
  "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""C:\Users\Device\GitReposMine\Kai-Mirror\ops\watchdog_keepalive.ps1""", 0, False
