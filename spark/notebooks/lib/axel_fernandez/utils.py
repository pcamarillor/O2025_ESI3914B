class BankAccount:
    def __init__(self, owner, balance=0):
        self.__owner = owner.lower()
        self.balance = balance

    def deposit(self, amount):
        if amount > 0:
            self.balance += amount
            print(f"Agregado {amount} al saldo")
        else:
            print("El monto del deposito debe ser positivo")

    def withdraw(self, amount):
        if 0 < amount <= self.balance:
            self.balance -= amount
            print(f"Retirado {amount} del saldo")
        else:
            print("Retiro invalido")

    def get_balance(self):
        return self.balance
    
    # Añadido personal para mostrar los datos extras como el nombre del dueño de la cuenta
    def __repr__(self):
        return f"BankAccount -> (owner={self.__owner}, balance={self.balance})"

# # Inicializamos la cuenta de banco con identificador de nombre y saldo por default en 0
# bank = BankAccount("Axel")

# bank_operations = {
#     'deposit': bank.deposit,
#     'withdraw': bank.withdraw,
#     'balance': bank.get_balance,
#     'account': bank.__repr__
# }

# # Realizamos ciertas operaciones con la estructura de datos con funciones

# print(bank_operations['account']())  # Iniciamos mostrando los datos de la cuenta con la funcion extra agregada.

# bank_operations['deposit'](100)  # Depositamos 100
# print(f'Balance despues del deposito: {bank_operations["balance"]()}')  # Mostramos el saldo reflejado

# bank_operations['withdraw'](50)  # Retiramos 50
# print(f'Balance despues del retiro: {bank_operations["balance"]()}')  # Mostramos el saldo reflejado

