import threading
from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.thread = None  # Store the worker's thread
    
    """
    Append t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)
        
    """
    Run all transaction as a thread
    """
    def run(self):
        # Create a thread that executes __run
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()
    
    """
    Wait for the worker to finish
    """
    def join(self):
        if self.thread:
            self.thread.join()

    def __run(self):
        for transaction in self.transactions:
            # Keep retrying until transaction commits
            while True:
                # each transaction returns True if committed or False if aborted
                result = transaction.run()
                if result:  # Transaction committed successfully
                    self.stats.append(True)
                    self.result += 1
                    break
                # else: Transaction aborted, retry