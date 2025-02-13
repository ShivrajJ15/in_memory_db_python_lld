

class Command:
    """
    Command pattern Interface for Transaction Operations.
    Each command must implement execute() and undo() methods.
    This allows transactions to be rolled back in case of failure.
    """

    def execute(self):
        raise NotImplementedError
    
    def undo(self):
        raise NotImplementedError
    

class SetCommand(Command):
    """
    Concrete Command class for SET operation.
    Stores the previous values to support undo() during rollbacks.
    """

    def __init__(self,shard,key,value,ex=None):
        self.shard=shard
        self.key=key
        self.value=value
        self.ex=ex
        self.prev_value=shard.get_key(key)   #stored the previous value for rollback

    def execute(self):
     """ Execute the  SET operation on the shard."""
     self.shard.set_key(self.key,self.value,self.ex)

    def undo(self):
         """Undo the self operation by restoring the previous values or deleting if None."""
         if self.prev_value is None:
             self.shard.delete_key(self.key)

         else:
             self.shard.set_key(self.key,self.value,self.ex)

            