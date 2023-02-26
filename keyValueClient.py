from pymemcache.client.base import Client

class KeyValueClient:
    def __init__(self):
        self.host = "0.0.0.0"
        self.port = 9889

    def setKey(self, key, value):
        try:
            client = Client(self.host + ":" + str(self.port), default_noreply=False)
            result = client.set(key, value)
            client.close()
            return result
        except:
            print()

    def getKey(self, key):
        try:
            client = Client(self.host + ":" + str(self.port))
            result = client.get(key)
            client.close()
            return result
        except Exception as e:
            print("exception in get key", e)
