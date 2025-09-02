class BankAccount:
    def __init__(self, balance):
        self.balance=abs(balance) # Balance inicial debe ser positivo

    def _deposit(self, amount):
        if amount>0:    # No depositar negativos
            self.balance+=amount
        else: raise ValueError("Error, the amount must be positive and more than zero")

    def _withdraw(self, amount):    # No retirar mas de lo que tenemos
        if amount<self.balance:
            self.balance-=amount
        else: raise ValueError("Error, your balance is not sufficient")
        
    def _getbalance(self):
        return self.balance