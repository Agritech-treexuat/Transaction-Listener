import time
from web3 import Web3
import datetime
from constants import event_map


class BlockchainListener:
    def __init__(self, endpoint, db):
        self.web3 = Web3(Web3.HTTPProvider(endpoint))
        print('Connected to blockchain:', self.web3.is_connected())
        self.db = db
        self.last_block_number = None

    async def listenToBlockchainEvents(self):
        try:
            
            while True:
                accounts = await self.db.getAccounts()
                print('Listening to blockchain events for accounts:', accounts)
                latest_block_number = self.web3.eth.get_block_number()
                print('Latest block number:', latest_block_number)

                if self.last_block_number is None or latest_block_number > self.last_block_number:
                    if self.last_block_number is not None:
                        start_block_number = self.last_block_number + 1
                    else:
                        start_block_number = latest_block_number - 1

                    if not self.last_block_number or latest_block_number > self.last_block_number:
                        for block_number in range(start_block_number, latest_block_number + 1):
                            block = self.web3.eth.get_block(block_number, True)
                            if block:
                                transactions = block.get('transactions', [])
                                for tx in transactions:
                                    # print('Checking transaction:', tx)
                                    # await self.handleEvent(None, tx, block['timestamp'])
                                    if tx['from'] in accounts or tx['to'] in accounts:
                                        await self.handleEvent(None, tx, block['timestamp'])

                        self.last_block_number = latest_block_number

                time.sleep(5)  # Chờ 5 giây trước khi lặp lại

        except Exception as e:
            print('Error listening to blockchain events:', e)
    
    # async def handleEvent(self, error, transaction):
    #     if not error:
    #         # Lưu giao dịch vào MongoDB
    #         await self.db.saveEvent(transaction)
    #     else:
    #         print('Error handle event:', error)
    
    async def handleEvent(self, error, transaction, timestamp=None):
        if not error:
            try:
                tx_hash = transaction['hash'].hex()
                block_number = transaction['blockNumber']
                tx = self.web3.eth.get_transaction(tx_hash)
                tx_from = tx['from']
                tx_value = self.web3.from_wei(tx['value'], 'ether')
                tx_fee = float(self.web3.from_wei(tx['gasPrice'] * tx['gas'], 'ether'))
                tx_timestamp_string = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') or datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                tx_timestamp = datetime.datetime.strptime(tx_timestamp_string, '%Y-%m-%d %H:%M:%S')
                
                # Lấy các logs từ giao dịch
                logs = self.web3.eth.get_logs({'transactionHash': tx_hash})
                # print("Logs:", logs)
                if logs:
                    event_name = 'other'
                    for log in logs:
                        for topic in log['topics']:
                            topic_hex = topic.hex()
                            if topic_hex in event_map:
                                event_name = event_map[topic_hex]['name']
                                break
                        if event_name != 'other':
                            break
                    print("=====================================")
                else:
                    # print("No event logs found.")
                    event_name = 'none'

                print("")
                print("Wallet:", tx_from)
                print("Transaction Hash:", tx_hash)
                print("Transaction Fee:", tx_fee, "ETH")
                print("Transaction Value:", tx_value, "ETH")
                print("Time:", tx_timestamp)
                print("Event:", event_name)

                # Lưu giao dịch vào MongoDB
                await self.db.saveEvent(tx_from, tx_fee, tx_timestamp, event_name, tx_hash)



            except Exception as e:
                print('Error processing event:', e)
        else:
            print('Error handle event:', error)


    def stopListening(self):
        print('Stopping listener...')
        # Không cần gọi unsubscribe vì không có subscription nào được tạo trong phương thức này
