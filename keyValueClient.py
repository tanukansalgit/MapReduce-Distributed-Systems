from pymemcache.client.base import Client
import time

class KeyValueClient:
    def __init__(self):
        self.host = "0.0.0.0"
        self.port = 9889

    def setKey(self, key, value):
        try:
            client = Client(self.host + ":" + str(self.port), default_noreply=False, encoding='utf-8')
            result = client.set(key, value)
            client.close()
            return result
        except:
            print('exception in set key')

    def getKey(self, key):
        try:
            client = Client(self.host + ":" + str(self.port))
            result = client.get(key)
            client.close()
            if result is None:
                return result
            return result.decode()
        except Exception as e:
            print("exception in get key", e)

    def delete(self):
        try:
            client = Client(self.host + ":" + str(self.port))
            result = client.delete("all")
            time.sleep(0.1)
        except:
            print('error in client delete function')
        return True
