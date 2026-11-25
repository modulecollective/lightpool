import json,os,signal,sys,time
from pathlib import Path
output=Path(sys.argv[1]); duration=float(sys.argv[2])
watched={signal.SIGTERM,signal.SIGHUP,signal.SIGINT}
signal.pthread_sigmask(signal.SIG_BLOCK,watched)
start=time.monotonic()
with output.open('x') as f:
 def emit(**data):
  f.write(json.dumps(dict(epoch=time.time(),elapsed=time.monotonic()-start,pid=os.getpid(),ppid=os.getppid(),sid=os.getsid(0),pgid=os.getpgrp(),**data))+'\n'); f.flush()
 emit(event='started',duration=duration)
 while time.monotonic()-start<duration:
  info=signal.sigtimedwait(watched,min(1,duration-(time.monotonic()-start)))
  if info:
   sender=Path(f'/proc/{info.si_pid}/comm')
   emit(event='signal',signal=info.si_signo,senderPID=info.si_pid,senderUID=info.si_uid,code=info.si_code,senderCommand=sender.read_text().strip() if info.si_pid>0 and sender.exists() else None)
   signal.pthread_sigmask(signal.SIG_UNBLOCK,watched)
   os.kill(os.getpid(),info.si_signo)
   raise SystemExit(128+info.si_signo)
  emit(event='heartbeat')
 emit(event='completed')
print('probe completed',duration)
