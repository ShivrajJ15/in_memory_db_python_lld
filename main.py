import threading
import time
from key_value_store import KeyValueStore

def basic_operations_demo(store):
    print("\n=== Basic Operations Demo ===")
    store.set("name", "TestName")
    print("Set name = TestName")
    
    value = store.get("name")
    print("Get name:", value)  # Expected Output: Alice
    
    store.set("age", 30)
    print("Set age = 30")
    
    value = store.get("age")
    print("Get age:", value)  # Expected Output: 30
    
    store.delete("age")
    print("Deleted age")
    
    value = store.get("age")
    print("Get age after delete:", value)  # Expected Output: None

def expiration_demo(store):
    print("\n=== Expiration Demo ===")
    store.set("session", "active", ex=2)
    print("Set session = active with expiration of 2 seconds")
    
    value = store.get("session")
    print("Get session (before expiration):", value)  # Expected Output: active
    
    time.sleep(3)  # Wait for expiration
    
    value = store.get("session")
    print("Get session (after expiration):", value)  # Expected Output: None

def transaction_demo(store):
    print("\n=== Transaction Demo ===")
    store.set("balance", 100)
    print("Initial balance = 100")
    
    store.begin()
    print("Begin transaction")
    
    store.set("balance", 150)
    print("Set balance = 150 within transaction")
    
    value = store.get("balance")
    print("Get balance within transaction:", value)  # Expected Output: 150
    
    store.rollback()
    print("Rollback transaction")
    
    value = store.get("balance")
    print("Get balance after rollback:", value)  # Expected Output: 100
    
    store.begin()
    print("Begin new transaction")
    
    store.set("balance", 200)
    print("Set balance = 200 within transaction")
    
    store.commit()
    print("Commit transaction")
    
    value = store.get("balance")
    print("Get balance after commit:", value)  # Expected Output: 200

def concurrency_demo(store):
    print("\n=== Concurrency Demo ===")
    
    def worker(thread_id):
        for i in range(5):
            key = f"counter_{thread_id}"
            current_value = store.get(key) or 0
            new_value = current_value + 1
            store.set(key, new_value)
            print(f"Thread-{thread_id}: Set {key} = {new_value}")
            time.sleep(0.1)

    threads = []
    for i in range(3):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()

    print("\nFinal Counter Values:")
    for i in range(3):
        key = f"counter_{i}"
        print(f"{key}:", store.get(key))  # Expected Output: 5 for each counter

if __name__ == "__main__":
    print("=== In-Memory Key-Value Store Demonstration ===")
    
    # Create an instance of the KeyValueStore
    store = KeyValueStore(num_shards=4)
    
    # Demonstrate basic CRUD operations
    basic_operations_demo(store)
    
    # Demonstrate expiration functionality
    expiration_demo(store)
    
    # Demonstrate transaction management
    transaction_demo(store)
    
    # Demonstrate concurrency handling with sharding
    concurrency_demo(store)