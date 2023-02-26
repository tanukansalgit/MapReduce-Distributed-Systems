import socket
import json
from _thread import *
import threading
import time
import os
import random

IFACE = "0.0.0.0"
PORT = "9889"
INITIAL_BUFFER_SIZE = 1024

DATA_FILE = "keyValue.json"

END_STRING = "\r\n"


kvData = {}

commands = {
  'SET' : "set",
  'GET' : "get"
}

response = {
  "stored" : "STORED",
  "notStored" : "NOT-STORED",
  "end" : "END"
}

def recieveData(sock):
  data = b''
  while(1):
    chunk = sock.recv(INITIAL_BUFFER_SIZE)
    data = data+ chunk
    if len(chunk) < INITIAL_BUFFER_SIZE:
        break

  return data.decode()

def getValue(key):
  result = []
  if key in kvData:
    result = kvData[key]

  return result


def setValue(key, value, flag, length):
  threadLockHandle.acquire()
  try:
    with open(DATA_FILE, 'w') as filename:
      kvData[key] = [value, flag, length]
      json.dump(kvData, filename)
      filename.close()
  except error:
    print('Exception: ', error)
  finally:
    threadLockHandle.release()


def handleClient(connection, isSleep):
  try:
    responseMessage = ""
    dataBytes = recieveData(connection).split("\r\n")

    print('dataBytes===', dataBytes)

    keyString = dataBytes[0].split(" ")
    valueString = "".join(dataBytes[1:])

    action = keyString[0]
    key = keyString[1]

    if isSleep:
      time.sleep(random.random())

    if action == commands['SET']:
      setValue(key, valueString, keyString[2], keyString[4])
      responseMessage = response["stored"] + END_STRING
    elif action == commands['GET']:
      val = getValue(key)
      if val and val[0]:
        firstMessage = f"VALUE {key} {val[1]} {val[2]}{END_STRING}"
        secondMessage = f"{val[0]}{END_STRING}"
        sendResponse(firstMessage, connection)
        sendResponse(secondMessage, connection)
      responseMessage = response["end"] + END_STRING
    else:
      raise Exception('Invalid command')

    sendResponse(responseMessage, connection)
  except error:
    print("Exception Occured : ", error)

  pass


def sendResponse(response, connection):
  try:
    connection.sendall(response.encode())
  except error:
    print('Exception:', error)
    pass

if __name__ == "__main__":
  global threadLockHandle

  print("Hello, I am a server")
  threadLockHandle = threading.Lock()
  addrInfo = socket.getaddrinfo(IFACE, PORT)

  socketType = socket.SOCK_STREAM

  with open(DATA_FILE, 'w+') as filename:
    if not os.stat(DATA_FILE).st_size:
       json.dump({}, filename)
       kvData = {}
    else:
      kvData = json.load(filename)
    filename.close()

  #Initialising socket
  connectionSocket = socket.socket(socket.AF_INET, socketType)
  connectionSocket.bind((addrInfo[0][-1][0], addrInfo[0][-1][1]))

  connectionSocket.listen()
  while(1):
    connection, address = connectionSocket.accept()
    print(f"connection from {address}")

    newThread = threading.Thread(target=handleClient, args=(connection, False))
    newThread.start()




