import subprocess,sys
code='''from pathlib import Path
import time
p=Path("/tmp/gossip-exec-cancel-heartbeat.json")
for i in range(150):
 p.write_text(str(i))
 time.sleep(.2)
'''
p=subprocess.Popen([sys.executable,'-c',code])
print('HEARTBEAT READY',flush=True)
try:p.wait(timeout=35)
finally:
 if p.poll() is None:
  p.terminate();p.wait(timeout=5)
