class BankAccount:
    def __init__(self, balance):
        self.balance=balance

        self.operations ={
            'deposit': self.deposit,
            'withdraw': self.withdraw,
            'get_balance': self.get_balance
        }

    def deposit(self, amount):
        self.balance=+amount
        return self.balance

    def withdraw(self, amount):
        if amount<=self.balance:
            self.balance=-amount
            return self.balance
        else:
            return "no se puede sacar mas de lo que tienes"


    def get_balance(self):
        return self.balance
    
    def process(self, operation_name, *args):
        operation = self.operations.get(operation_name)
        return operation(*args)