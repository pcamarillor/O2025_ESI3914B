class BankAccount:
    def __init__ (self, saldo):
        self.saldo = saldo
        self.operations = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance,
        }

    def deposit(self, monto):
        self.saldo += monto
        return f"Depositaste un monto de {monto} a la cuenta. Nuevo saldo: {self.saldo}"
    
    def withdraw(self, monto):
        if monto > self.saldo:
            return "Saldo insuficiente"
        if monto < self.saldo:
            self.saldo -= monto
            return f"Retiraste un monto de {monto} a la cuenta. Nuevo saldo: {self.saldo}"
    
    def get_balance(self):
        return f"El saldo actual es de {self.saldo}"

    def process(self, operacion, monto = None):
        if operacion not in self.operations:
            return "Operacion Invalida"
        if monto is not None:
            return self.operations[operacion](monto)
        else:
            return self.operations[operacion]()