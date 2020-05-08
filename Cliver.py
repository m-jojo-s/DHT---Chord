#       Working: all without GUI
#=====================================================
import threading, socket, json, sys, time
from sys import argv, exit
from os import system, listdir, stat, name, remove
from hashlib import sha1

class Node(object):

    def __init__(self, port = 0):
        self.ip = socket.gethostbyname(socket.gethostname())
        self.port = port
        self.id = -1
        self.pred = {"id": self.id, "ip": self.ip, "port": self.port}
        self.fTable = {}
        self.sTable = {}
        self.files = []
        for i in range(0, m):
            self.fTable[i] = self.pred
            succ = self.pred.copy()
            succ["noReply"] = 0
            self.sTable[i] = succ
        for file in listdir(filesPath):
            key = Hash(file)
            fStats = stat(filesPath + file)
            entry = {"id": key, "name": file, "size": fStats.st_size, "totalSize": fStats.st_size}
            self.files.append(entry)
    
    def Send(self, nodeTo, rqst):
        fail = 0
        while fail < 3:
            try:
                rqst["sentFrom"] = self.id
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                sock.connect((nodeTo["ip"], int(nodeTo["port"])))
                sock.send(json.dumps(rqst).encode())
                sock.close()
                return
            except (ConnectionRefusedError, TimeoutError, socket.timeout):
                #print("connection refused")
                #print("to:", nodeTo, "rqst:", rqst)
                fail = fail + 1
                pass
            except KeyError:
                print("to:", nodeTo, "rqst:", rqst)
                print("Key error while sending")
                return
    
    #Request of form: {type: ...., rqst[type]: {name:, size: 0, totalSize:}, offset:}
    def SendFile(self, nodeTo, rqst):
        fail = 0
        offset = rqst["offset"]
        while fail < 3:
            try:
                rqst["sentFrom"] = self.id
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((nodeTo["ip"], int(nodeTo["port"])))
                
                dwnRqst = {"type": "downFile", "downFile": rqst[rqst["type"]], "from": self.id}
                sock.send(json.dumps(dwnRqst).encode())
                if sock.recv(64).decode() != "OK": # recieve "OK" ready signal
                    sock.close()
                    return
                
                fileName = rqst[rqst["type"]]["name"]
                with open(filesPath + fileName, 'rb') as f:
                    stats = stat(filesPath + fileName)
                    sent = sock.sendfile(f,offset)
                    print("File sent:", sent, "of", stats.st_size, "Bytes.")
                    sock.close()
                return
            except ConnectionRefusedError:
                #print("connection refused")
                #print("to:", nodeTo, "rqst:", rqst)
                fail = fail + 1
            except BrokenPipeError:
                print("Connection lost while transferring file")
                return
            except:
                print("Error while sending file.")
                return
    
    def RecieveFile(self, sock, rqst):
        try:
            index = len(self.files)
            fileName = rqst[rqst["type"]]["name"]
            
            present = False
            
            for file in self.files:
                if file["name"] == fileName:
                    present = True
                    index = self.files.index(file)
            if not present: self.files.append(rqst[rqst["type"]])
            
            scale = 1024
            
            f = open(filesPath + fileName, "ab") #Append to file in binary mode
            self.files[index]["size"] = f.tell()
            sock.send("OK".encode()) #Send ready-signal to sender
            print("\nDownloading file...")
            piece = sock.recv(1000*scale)
            while piece and f.tell() < rqst[rqst["type"]]["totalSize"]:
                f.write(piece)
                print(f.tell(), "of", rqst[rqst["type"]]["totalSize"], "Bytes")
                self.files[index]["size"] = f.tell()
                piece = sock.recv(1000*scale)
            print("Downloaded:", f.tell(), "of", rqst[rqst["type"]]["totalSize"], "Bytes")
            print("From:", rqst["from"])
            f.close()
        except:
            print("Error while recieving file")
            return
        
    #Find which finger to forward request to
    def Forward(self, rqst):
        #======================================
        checkID = rqst[rqst["type"]]["id"] #FORMAT all requests accordingly
        #======================================
        finger = 0
        pred = self.id
        #Check with immediate successor
        if self.CheckSucc(checkID, pred, self.fTable[0]["id"]):
            to = {"ip": self.fTable[0]["ip"], "port": self.fTable[0]["port"]}
            self.Send(to, rqst)
        else:
            for i in range(m-1, 0, -1):
                if self.fTable[i]["id"] == self.id:
                    continue
                id = self.fTable[i]["id"]
                if self.CheckSucc(id, pred, checkID):
                    finger = i
                
            to = {"ip": self.fTable[finger]["ip"], "port": self.fTable[finger]["port"]}
            self.Send(to, rqst)

    #Used for replicating files
    def PutAll(self, to = {}):
        for file in self.files:
            id = file["id"]
            rqst = {"type": "put", "put": {"id": id, "name": file["name"]}, "id": self.id, "ip": self.ip, "port": self.port}
            self.Forward(rqst) if to == {} else self.Send(to,rqst)

    def UpdatePred(self, pred):
        self.pred = pred
        self.PutAll({"ip": pred["ip"], "port": pred["port"]})
    
    def UpdateSucc(self, succ):
        self.fTable[0] = succ
        self.sTable[0] = succ.copy()
        self.sTable[0]["noReply"] = 0

    #Used after starting server to make sure server and node properties match
    def Update(self):
        self.id = Hash(self.ip + str(self.port))
        self.pred = {"id": self.id, "ip": self.ip, "port": self.port}
        for i in range(0, m):
            self.fTable[i] = {"id": self.id, "ip": self.ip, "port": self.port}
            self.sTable[i] = {"id": self.id, "ip": self.ip, "port": self.port, "noReply": 0}

    def UpdateFinger(self, finger):
        i = finger["num"]
        del finger["num"]
        self.fTable[i] = finger
        self.PutAll({"ip": finger["ip"], "port": finger["port"]})

    def UpdateS(self, query):
        i = query["num"]
        del query["num"]
        self.sTable[i] = query
        if i == 0:
            self.fTable[0] = self.sTable[0].copy()
        self.sTable[i]["noReply"] = 0
            
    def UpdateFTable(self):
        #=========Uncomment for update thread=======
        global chalo
        while (True):
            time.sleep(m) #Sleep for m seconds
            if not chalo:
                return
            self.fTable[0] = self.sTable[0].copy()
            del self.fTable[0]["noReply"] #Relying on successor table to keep correct immediate succesor
        #===========================================
            for i in range(1, m):
                finger = {"num": i, "id": (self.id + 2**i) % maxNodes, "ip": self.ip, "port": self.port} # finding for -1 to reuse CheckSucc code
                rqst = {"type": "findF", "findF": finger}
                to = {"ip": self.fTable[0]["ip"], "port": self.fTable[0]["port"]}
                self.Send(to, rqst)
        
    def UpdateSTable(self):
        global chalo
        while (True):
            time.sleep(5) #Sleep for 5 seconds
            if not chalo:
                return
            temp = self.sTable.copy()
            temp[-1] = {"id": self.id, "ip": self.ip, "port": self.port, "noReply": 0}
            for i in range(0, m):
                query = {"num": i, "id": (temp[i-1]["id"] + 1) % maxNodes, "ip": self.ip, "port": self.port}
                rqst = {"type": "findS", "findS": query}
                to = {"ip": self.sTable[i]["ip"], "port": self.sTable[i]["port"]}
                self.Send(to, rqst)
                self.sTable[i]["noReply"] = self.sTable[i]["noReply"] + 1
            #Sort table based on least nonReplies and closest successor
            #self.sTable = STableSort(self.sTable, "noReply", "id", self.id)
            self.fTable[0] = self.sTable[0].copy()
            del self.fTable[0]["noReply"]
            for i in range(0, m):
                #If no replies >=4 : shift table up from i onwards
                if self.sTable[i]["noReply"] >= 4:
                    for j in range(i, m-1):
                        self.sTable[j] = self.sTable[j+1]
                    #Add new entry = new immediate successor
                    self.sTable[m-1] = {"id": self.sTable[0]["id"], "ip": self.sTable[0]["ip"],
                                        "port": self.sTable[0]["port"], "noReply": 0}
                    
            #Tell immediate successor that i'm your new predecessor and put files
            to = {"ip": self.sTable[0]["ip"], "port": self.sTable[0]["port"]}
            rqst = {"type": "pred", "pred":{"id": self.id, "ip": self.ip, "port": self.port}}
            self.Send(to, rqst)
            self.PutAll(to)

    def CheckSucc(self, checkID, pred, id):
        if pred == id or checkID == id:
            return True
        i = pred + 1
        while True:
            if i == checkID:
                return True
            elif i == id:
                return False
            i = (i+1) % maxNodes
    
    def Find(self, find, typeChar):
        checkID = find["id"]
        if self.CheckSucc(checkID, self.pred["id"], self.id):
            to = {"ip": find["ip"], "port": find["port"]}
            rqst = {"type": "update" + typeChar, "update" + typeChar: {"id": self.id, "ip": self.ip, "port": self.port, "num": find["num"]}}
            self.Send(to, rqst)
        else:
            rqst = {"type": "find" + typeChar, "find" + typeChar: find}
            self.Forward(rqst)

    def Join(self, join):    
        #For finger table
        jID = join["id"]
        pred = self.pred["id"]
        
        me = self.CheckSucc(jID, self.pred["id"], self.id)

        if self.id == pred or me:
            #print("I am your successor")
            succ = {"id": self.id, "ip": self.ip, "port": self.port}
            pred = {"id": self.pred["id"], "ip": self.pred["ip"], "port": self.pred["port"]}
            
            #Update own pred
            self.UpdatePred({"id": jID, "ip": join["ip"], "port": join["port"]})
            #Tell pred to update its succ and pred
            self.Send({"ip": join["ip"], "port": join["port"]}, {"type": "succpred", "succ": succ, "pred": pred})
            return
        else:
            #print("forwarding to", self.fTable[0]["id"])
            self.Forward({"type": "join", "join": join})
            return

    def Leave(self):   
        global chalo     
        succ = self.fTable[0]
        self.Send({"ip": succ["ip"], "port": succ["port"]}, {"type": "pred", "pred": self.pred})
        self.Send({"ip": self.pred["ip"], "port": self.pred["port"]}, {"type": "succ", "succ": succ})
        
        if succ["id"] != self.id:
            #Transfer files to successor
            print("Transferring files to successor...")
            for file in self.files:
                rqst = {"type": "upFile", "upFile":{"id": Hash(file["name"]), "name": file["name"]}, "offset": 0, "ip": succ["ip"], "port": succ["port"]}
                self.Send({"ip": self.ip, "port": self.port}, rqst)
        
        chalo = False
        self.Send({"ip": socket.gethostbyname(socket.gethostname()), "port": self.port}, {"type": "leave"})


#Returns str hashed to int id
def Hash(str):
    #Same id for port 8000 and 34673?
	key = sha1(str.encode())	
	return int(key.hexdigest(), 16)  % maxNodes #Convert key to it's respective hex value and return it's respective int value
#Returns a dictionary sorted by two fields
def STableSort(dict, sortBy0, sortBy1, id):
    arr = []
    for i in range(0, len(dict)):
        arr.append(dict[i])
    maxID = max(arr, key = lambda entry: entry[sortBy1])["id"]
    arr.sort(key = lambda entry: (entry[sortBy0], (entry[sortBy1] if entry[sortBy1] >= id else entry[sortBy1] + maxID)))
    sorted = {}
    for i in range(0, len(dict)):
        sorted[i] = arr[i]
    return sorted
#Process different request types for server
def Request(rqst, sock, node):
    #print("\nServer: processing request", rqst)
    
    if   rqst["type"] == "msg":
        print("\n" + rqst["msg"])
        return
    
    elif rqst["type"] == "join":
        node.Join(rqst["join"])
        
    elif rqst["type"] == "pred":
        node.UpdatePred(rqst["pred"])
        
    elif rqst["type"] == "succ":
        node.UpdateSucc(rqst["succ"])

    elif rqst["type"] == "succpred":
        node.UpdatePred(rqst["pred"])
        node.UpdateSucc(rqst["succ"])
        to = {"ip": rqst["pred"]["ip"],  "port": rqst["pred"]["port"]}
        node.Send(to, {"type": "succ", "succ": {"id": node.id, "ip": node.ip, "port": node.port}})
    
    elif rqst["type"] == "updateFTable":
        node.UpdateFTable()
    
    elif rqst["type"] == "findF":
        node.Find(rqst["findF"], "F")
    
    elif rqst["type"] == "findS":
        node.Find(rqst["findS"], "S")
    
    elif rqst["type"] == "updateF":
        node.UpdateFinger(rqst["updateF"])

    elif rqst["type"] == "updateS":
        node.UpdateS(rqst["updateS"])

    elif rqst["type"] == "upFile":
        found = False
        for file in node.files:
            if rqst["upFile"]["name"] == file["name"] and file["size"] == file["totalSize"]:
                found = True
                rqst["upFile"] = file.copy()
                node.SendFile({"ip": rqst["ip"], "port": rqst["port"]}, rqst)
        if not found:
            if node.CheckSucc(rqst["upFile"]["id"], node.pred["id"], node.id):
                node.Send({"ip": rqst["ip"], "port": rqst["port"]}, {"type": "msg", "msg": "File " + rqst["upFile"]["name"] + " not found!"})
            else: 
                node.Forward(rqst)
                
    elif rqst["type"] == "downFile":
        for file in node.files:
            if rqst["downFile"]["name"] == file["name"] and file["size"] >= file["totalSize"]:
                #print("\nFile already present!")
                sock.send("NO! >:(".encode())
                return
        node.RecieveFile(sock, rqst)
    
    elif rqst["type"] == "put":
        
        if rqst["sentFrom"] == node.id:
            return
        id = rqst["put"]["id"]
        
        #Node is supposed to keep a copy if it is a finger or predecessor
        keep = False
        if node.pred["id"] == rqst["id"]:
            keep = True
        for i in range(0, m):
                finger = (rqst["id"] + 2**i) % maxNodes
                if node.CheckSucc(finger, node.pred["id"], node.id):
                    keep = True
        if keep:
            offset = 0
            for file in node.files:
                if  file["name"] == rqst["put"]["name"]:
                    return
            
            #Tell node to upload file here
            msg = {"type": "upFile", "upFile":{"id": id, "name": rqst["put"]["name"]}, "offset": offset, "ip": node.ip, "port": node.port}
            node.Send({"ip": rqst["ip"], "port": rqst["port"]}, msg)
        else:
            print("Not keeping", rqst["put"]["name"])
            node.Forward(rqst)
    
    try:
        #Close socket after processing request
        sock.shutdown(socket.SHUT_RD)
        sock.close()
    except OSError:
        return
#Accept connections, start request processing
def Server(node, port = 0, join = ""):
    host = socket.gethostname()

    #creating socket
    srvSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srvSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #Flag socket to close port immediately after disconnection

    try:
        srvSocket.bind((host,port))
        port = srvSocket.getsockname()[1]
        #print("\nServer:", srvSocket.getsockname()[0], "Running on port", port)
        node.port = port
        node.Update()
    except OSError as err:
        print(err, "\nServer: Unable to bind to port:", port, "on", srvSocket.getsockname()[0])
        srvSocket.shutdown(socket.SHUT_RDWR)
        srvSocket.close()
        exit()

    #Start thread for updating finger and successortable after every m seconds
    threadF, threadS = threading.Thread(target = node.UpdateFTable), threading.Thread(target = node.UpdateSTable)
    threads.append(threadF)
    threads.append(threadS)
    threadF.start()
    threadS.start()
    
    if join != "":
            rqst = {"type": "join", "join": {"id": node.id, "ip": node.ip, "port": port}}
            node.Send(join, rqst)
    srvSocket.listen()
    while(True):
        conn = srvSocket.accept()[0]
        conn.settimeout(5)
        
        #Recieve and then parse message as json string
        try:
            rcvd = conn.recv(1024).decode()
            #print("\nServer: Received Query from", conn.getpeername()[0], "at port:", conn.getpeername()[1])
            rqst = json.loads(rcvd)
            if rqst["type"] == "leave":
                print("\nLeaving")
                print("waiting for threads to finish")
                for thread in threads:
                    thread.join()
                break
        except socket.timeout as err:
            continue     
        except json.JSONDecodeError as err:  
            #rqst = {"type": "message", "msg": rcvd}
            print(rcvd)
            continue
        
        thread = threading.Thread(target = Request, args = (rqst, conn, node,))
        threads.append(thread)
        thread.start()

def Clear():
	system('cls' if name=='nt' else 'clear')

def Client(mode):
    global chalo
    print("IP:", socket.gethostbyname(socket.gethostname()), "Port:",selfNode.port)
    print("ID:", selfNode.id, "Predecessor:", selfNode.pred["id"])
    
    if mode == 2:
        print("\nFinger Table:")
        for i in range(0, len(selfNode.fTable)):
            print("For:", (selfNode.id + 2**i) % maxNodes, selfNode.fTable[i])
            
    elif mode == 3:
        #selfNode.Send({"ip": socket.gethostbyname(socket.gethostname()), "port": selfNode.port}, {"type": "updateFTable"})
        #Successor table:
        print("\nSuccessor Table:")
        for i in range(0, len(selfNode.sTable)):
            print(selfNode.sTable[i])
   
    elif mode == 4:
        fileName = input("Enter a file to search for (0 to cancel): ")
        if fileName == "0":
            return
        offset = 0
        for file in selfNode.files:
            if fileName == file["name"] and file["size"] != file["totalSize"]:
                offset = file["size"]
                choice = input("\nFile present. Would you like to continue downloading? (Y/N): ")
                if offset == file["totalSize"] and (choice == "Y" or choice == "y"):
                    remove(filesPath + fileName)
                    del selfNode.files[selfNode.files.index(file)]
                    offset = 0
                elif choice == "N" or choice == "n":
                    return
                
        id = Hash(fileName)
        rqst = {"type": "upFile", "upFile":{"id": id, "name": fileName}, "offset": offset, "ip": selfNode.ip, "port": selfNode.port}
        selfNode.Send({"ip": selfNode.ip, "port": selfNode.port}, rqst)
        
    elif mode == 5:
        print("\nFiles:")
        for file in selfNode.files:
            print("ID:", file["id"], "Name:", file["name"], "Size on disk:", file["size"], "Expected size:", file["totalSize"])
           
    elif mode == 0:     
        selfNode.Leave()
m = 7
maxNodes = 2**m #Set max number of nodes to 2^m
chalo = True
filesPath = "Files/"

#Possible command-line arguments: python3 fileName, ServerStartingPort, *nodeJoinIP, *nodeJoinPort
port = 0        #From documentation: If host or port are ‘’ or 0 respectively the OS default behavior will be used
threads = []    #All threads started by server
join = ""

if len(argv) == 3:
	print("Error: not enough information for joining network!")
	exit(1)
 
elif len(argv) == 2:
    port = int(argv[1])

if len(argv) == 4:
    port = int(argv[1])
    join = {"ip": argv[2], "port": argv[3]}

selfNode = Node(port)

#Start server
thread = threading.Thread(target = Server, args = (selfNode, selfNode.port, join))
thread.start()
        
#Client
mode = 1
chalo = True
while(chalo):
    Client(mode)
    if mode == 0:
        break
    try:
        mode = int(input("\nOptions: \n1) Refresh \n2) Display finger table \n3) Display successor table \n4) Search for a file \n5) List of available files\n0) Quit \nChoice: "))
        Clear()
    except:
        mode = 0
        
#python3 Cliver.py 0 127.0.1.1 3000