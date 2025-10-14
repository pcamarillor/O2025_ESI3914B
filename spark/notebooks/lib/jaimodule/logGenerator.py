import random
import time
import os

route = "/opt/spark/work-dir/data/logs"

Logtype = ["App1","App2","App3","App4"]
Errortype = ["Success","Fail","Error","Unauthorized"]

def generate():
    start_time = time.time()
    while True:
        if time.time()-start_time > 60:
            break
        
        content_list = []

        for i in range(random.randint(5,15)):
            line = Logtype[random.randint(0,3)] + str(time.time())+" "+Errortype[random.randint(0,3)]
            content_list.append(line)

        content = "\n".join(content_list)+"\n"

        docu = os.path.join(route, "log"+str(int(time.time()))+".txt")
        
        file = open(docu, "w")
        file.write(content)
        file.close()

        print("Archivo Escrito")

        time.sleep(5)

