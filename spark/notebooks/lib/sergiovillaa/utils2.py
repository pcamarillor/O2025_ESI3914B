class BankAccount:
    def __init__(self, balance):
        self._balance = balance

    def deposit(self, n):
        self.balance += n
    
    def withdraw(self, n):
        self.balance -= n

    