class BankAccount:
    def __init__(self, initial_balance=0):
        self._balance = initial_balance
        
        self.operations = {
            'deposit': self._deposit,
            'withdraw': self._withdraw,
            'get_balance': self._get_balance
        }
    
    def _deposit(self, amount):
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")
        
        self._balance += amount
        print(f"Deposited ${amount}. New balance: ${self._balance}")
        return self._balance
    
    def _withdraw(self, amount):
        if amount <= 0:
            raise ValueError("Withdrawal amount must be positive")
        
        if amount > self._balance:
            raise ValueError(f"Insufficient funds. Attempted to withdraw ${amount}, but only ${self._balance} available")
        
        self._balance -= amount
        print(f"Withdrew ${amount}. New balance: ${self._balance}")
        return self._balance
    
    def _get_balance(self):
        print(f"Current balance: ${self._balance}")
        return self._balance
    
    def process(self, operation_name, *args):
        if operation_name not in self.operations:
            available = ", ".join(self.operations.keys())
            raise KeyError(f"Operation '{operation_name}' not found. Available: {available}")
        
        operation_func = self.operations[operation_name]
        return operation_func(*args)
    
    def get_available_operations(self):
        return list(self.operations.keys())
    
    @property
    def balance(self):
        return self._balance
    
    def __str__(self):
        return f"BankAccount(Balance: ${self._balance})"
    
    def __repr__(self):
        return f"BankAccount({self._balance})"
