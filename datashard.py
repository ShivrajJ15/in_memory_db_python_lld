
import time
import threading

class DataShard:
    """
    Represents a Shard for Partitioning Data Store.
    Each shard has its own store, expiration management, and lock for concurrency control.
    """
    def __init__(self):
        self.store = {}        # Dictionary to store key-value pairs
        self.expiry = {}       # Dictionary to store expiration times
        self.lock = threading.Lock()  # Lock for thread safety within the shard
        self.log = []          # Append-only log for persistence

    def set_key(self, key, value, ex=None):
        """
        Sets a key-value pair in the shard with optional expiration.
        :param key: Key to store
        :param value: Value to store
        :param ex: Expiration time in seconds (optional)
        """
        with self.lock:
            self.store[key] = value
            if ex:
                self.expiry[key] = time.time() + ex
            self.persist_log(f"SET {key} {value} {ex}")

    def get_key(self, key):
        """
        Retrieves the value of a key if not expired.
        :param key: Key to retrieve
        :return: Value or None if key is expired or not found
        """
        with self.lock:
            if key in self.store and (key not in self.expiry or time.time() < self.expiry[key]):
                return self.store[key]
            return None

    def delete_key(self, key):
        """
        Deletes a key from the shard.
        :param key: Key to delete
        """
        with self.lock:
            if key in self.store:
                del self.store[key]
                self.persist_log(f"DEL {key}")

    def persist_log(self, operation):
        """
        Appends the operation to the log for persistence.
        :param operation: Operation string (e.g., "SET key value")
        """
        self.log.append(operation)