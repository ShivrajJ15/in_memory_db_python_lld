
import time
import threading

class Transaction:
    """
    Represents a Transaction with a list of commands.
    Supports nested transactions and rollback using Command Pattern.
    """
    def __init__(self):
        self.commands = []

    def add_command(self, command):
        """
        Adds a command to the current transaction.
        :param command: Command instance (e.g., SetCommand)
        """
        self.commands.append(command)

    def commit(self):
        """
        Commits the transaction by executing all commands sequentially.
        """
        for command in self.commands:
            command.execute()

    def rollback(self):
        """
        Rolls back the transaction by undoing all commands in reverse order.
        This ensures nested transactions are correctly reverted.
        """
        for command in reversed(self.commands):
            command.undo()


class TransactionManager:
    """
    Manages Transaction Lifecycle.
    Supports nested transactions and maintains isolation.
    Implements the Singleton Pattern for centralized transaction management.
    """
    def __init__(self):
        self.transactions = []

    def begin(self):
        """
        Begins a new transaction and pushes it onto the stack.
        Supports nested transactions.
        """
        self.transactions.append(Transaction())

    def add_command(self, command):
        """
        Adds a command to the current active transaction.
        Raises an error if no active transaction is found.
        """
        if not self.transactions:
            raise RuntimeError("No active transaction.")
        self.transactions[-1].add_command(command)

    def commit(self):
        """
        Commits the most recent transaction from the stack.
        """
        if not self.transactions:
            raise RuntimeError("No active transaction to commit.")
        transaction = self.transactions.pop()
        transaction.commit()

    def rollback(self):
        """
        Rolls back the most recent transaction from the stack.
        """
        if not self.transactions:
            raise RuntimeError("No active transaction to rollback.")
        transaction = self.transactions.pop()
        transaction.rollback()
