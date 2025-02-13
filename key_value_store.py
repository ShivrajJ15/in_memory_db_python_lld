
from datashard import DataShard
from transactions import TransactionManager
from command import SetCommand


class KeyValueStore:
    """
    Main Class to Manage Operations and Sharding.
    Implements horizontal scaling using consistent hashing for sharding.
    """
    def __init__(self, num_shards=4):
        self.shards = [DataShard() for _ in range(num_shards)]
        self.transaction_manager = TransactionManager()

    def _get_shard(self, key):
        """
        Determines the shard for the given key using consistent hashing.
        :param key: Key to determine shard
        :return: Corresponding DataShard instance
        """
        return self.shards[hash(key) % len(self.shards)]

    def set(self, key, value, ex=None):
        """
        Sets a key-value pair in the appropriate shard.
        Supports transactions using Command Pattern.
        """
        shard = self._get_shard(key)
        command = SetCommand(shard, key, value, ex)
        if self.transaction_manager.transactions:
            self.transaction_manager.add_command(command)
        else:
            command.execute()

    def get(self, key):
        """
        Retrieves the value for the given key.
        :param key: Key to retrieve
        :return: Value or None if key is expired or not found
        """
        shard = self._get_shard(key)
        return shard.get_key(key)

    def delete(self, key):
        """
        Deletes a key from the appropriate shard.
        :param key: Key to delete
        """
        shard = self._get_shard(key)
        shard.delete_key(key)

    def begin(self):
        """Begins a new transaction."""
        self.transaction_manager.begin()

    def commit(self):
        """Commits the current transaction."""
        self.transaction_manager.commit()

    def rollback(self):
        """Rolls back the current transaction."""
        self.transaction_manager.rollback()