import threading
from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import LockManager, LockException

# Global lock manager shared by all transactions
_global_lock_manager = LockManager()

# Thread-local storage for current transaction ID
_current_transaction = threading.local()

def get_current_txn_id():
    """Get current transaction ID from thread-local storage."""
    if not hasattr(_current_transaction, 'txn_id'):
        _current_transaction.txn_id = None
    return _current_transaction.txn_id


class Transaction:
    # Class-level transaction ID counter
    _txn_counter = 0
    _counter_lock = threading.Lock()

    """
    # Creates a transaction object.
    """
    def __init__(self):
        # Generate unique transaction ID
        with Transaction._counter_lock:
            self.txn_id = Transaction._txn_counter
            Transaction._txn_counter += 1
        
        self.queries = []
        self.lock_manager = _global_lock_manager
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # Set thread-local transaction ID so queries can acquire locks
        _current_transaction.txn_id = self.txn_id
        
        try:
            for query, args in self.queries:
                result = query(*args)
                # If the query has failed the transaction should abort
                if result == False:
                    return self.abort()
            return self.commit()
        except LockException:
            # Lock conflict occurred, abort transaction
            return self.abort()
        finally:
            # Clean up thread-local storage
            _current_transaction.txn_id = None

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        # Release all locks held by this transaction
        self.lock_manager.release_all(self.txn_id)
        return False

    
    def commit(self):
        # TODO: commit to database
        # Release all locks held by this transaction
        self.lock_manager.release_all(self.txn_id)
        return True

