from threading import Timer
import time
def p(s):
    print(s)

t = Timer(0, p, ("Started receiving data! Fingers crossed...",))
t.start()

def cool(t):
    try:
        print('cool')
        print(t)
        x + 5
    except:
        print('fuck')

while True:
    if not t.is_alive():
        t = Timer(10, cool, (time.time(),))
        t.start()   
    else: 
        print('working')
        time.sleep(1)

t.start()