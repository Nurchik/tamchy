from Client import Client
import os

if __name__ == '__main__':
    c = Client()
    while c.work:
        pass
    # user clicked exit button
    c.close()
    # 1 --> SIGHUP
    os.kill(os.getppid(),1)