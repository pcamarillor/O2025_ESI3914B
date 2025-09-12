class BankAccount:
    def __init__(self, balance):
        self.balance=abs(balance) # Balance inicial debe ser positivo

        self._operations={      # Diccionario para mapear las funciones doble guion para hacerlos mas dificiles de acceder
            "deposit": self.__deposit,
            "withdraw": self.__withdraw,
            "get_balance": self.__getbalance
        }

    def __deposit(self, amount):
        if amount>0:    # No depositar negativos
            self.balance+=amount
        else: raise ValueError("Error, the amount must be positive and more than zero")

    def __withdraw(self, amount):    # No retirar mas de lo que tenemos
        if amount<=self.balance:
            self.balance-=amount
        else: raise ValueError("Error, your balance is not sufficient")
        
    def __getbalance(self):
        return self.balance
    
    def process(self, operation, *args):    # Con process se accede mas facilmente a los metodos que en teoria por el __ son privados
        if operation in self._operations:
            return self._operations[operation](*args)
        else: raise ValueError("Not supported")