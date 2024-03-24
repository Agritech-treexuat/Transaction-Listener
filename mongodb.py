from pymongo import MongoClient

class MongoDB:
    def __init__(self, url, dbName):
        self.url = url
        self.dbName = dbName
        self.client = None
        self.db = None

    async def connect(self):
        try:
            self.client = MongoClient(self.url)
            self.db = self.client[self.dbName]
            print('Connected to MongoDB')
        except Exception as e:
            print('Error connecting to MongoDB:', e)
            raise e

    async def disconnect(self):
        try:
            self.client.close()
            print('Disconnected from MongoDB')
        except Exception as e:
            print('Error disconnecting from MongoDB:', e)
            raise e

    async def getAccounts(self):
        try:
            collection = self.db['Farms']  # Lấy collection "Farms"
            accounts = list(collection.find({'status': 'active'}, {'walletAddress': 1, '_id': 0}))  # Chỉ lấy trường "account"
            accounts_list = []
            for account in accounts:  # Await the cursor iteration
                if 'walletAddress' in account and account['walletAddress'] != '' and account['walletAddress']:
                    accounts_list.append(account['walletAddress'])
            return accounts_list  # Trả về danh sách các tài khoản
        except Exception as e:
            print('Error getting accounts from MongoDB:', e)
            raise e


    async def saveEvent(self, tx_from, tx_fee, tx_timestamp, event_name, tx_hash):
        try:
            collection = self.db['Events']
            collection.insert_one({
                'walletAddress': tx_from,
                'fee': tx_fee,
                'timestamp': tx_timestamp,
                'event': event_name,
                'tx': tx_hash
            })
            print('Event saved to MongoDB:', tx_from, tx_fee, tx_timestamp, event_name, tx_hash)
        except Exception as e:
            print('Error saving event to MongoDB:', e)
            raise e
