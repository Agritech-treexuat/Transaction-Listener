import asyncio
import signal
import sys
from pymongo import MongoClient
from blockchain_listener import BlockchainListener
from mongodb import MongoDB

# Thông tin kết nối MongoDB
MONGODB_URL = "mongodb://huy:12345678@localhost:27017"
DB_NAME = "AgriDB"

# Thông tin kết nối Blockchain
BLOCKCHAIN_ENDPOINT = "https://evmos-pokt.nodies.app"

# Khởi tạo biến listener để đảm bảo nó được khai báo trước khi sử dụng
listener = None
db = None

async def main():
    global listener  # Đảm bảo chúng ta sử dụng biến listener ở phạm vi toàn cục
    global db  # Đảm bảo chúng ta sử dụng biến db ở phạm vi toàn cục
    try:
        # Khởi tạo kết nối MongoDB
        db = MongoDB(MONGODB_URL, DB_NAME)
        await db.connect()

        # Lấy danh sách các tài khoản từ MongoDB
        accounts = await db.getAccounts()
        print('Accounts:', accounts)

        # Khởi tạo lớp lắng nghe blockchain
        listener = BlockchainListener(BLOCKCHAIN_ENDPOINT, db)

        # Bắt đầu lắng nghe sự kiện từ blockchain cho các tài khoản đã được lấy
        await listener.listenToBlockchainEvents(accounts)

        # Bắt tín hiệu kết thúc chương trình (Ctrl+C)
        signal.signal(signal.SIGINT, signal_handler)

        # Đợi cho đến khi người dùng kết thúc chương trình
        await asyncio.sleep(99999999)  # Dừng vô hạn

    except Exception as e:
        print("An error occurred:", e)

    finally:
        # Dừng lắng nghe và đóng kết nối MongoDB
        if db:
            await db.disconnect()
        if listener:
            listener.stopListening()

def signal_handler(sig, frame):
    print("Exiting...")
    if db:
        asyncio.create_task(db.disconnect())  # Create a task for disconnect
    if listener:
        listener.stopListening()
    # asyncio.ensure_future(main_task.cancel())  # Hủy công việc chính
    sys.exit(0)

if __name__ == "__main__":
    try:
        # Chạy công việc chính bằng cách await main()
        asyncio.run(main())
    except KeyboardInterrupt:
        # Xử lý khi người dùng ấn Ctrl+C
        print("KeyboardInterrupt: Stopping listener...")
        if db:
            db.disconnect()
        if listener:
            listener.stopListening()
