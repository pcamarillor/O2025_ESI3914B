class BankAccount:
    def __init__(self, owner, balance=0):
        # Dejamos todos los atributos privados para evitar modificaciones directas
        self.__owner = owner.lower()
        self.__balance = balance
        self.__operations = {
            'deposit': self.__deposit,
            'withdraw': self.__withdraw,
            'balance': self.__get_balance,
        }

    # Metodos privados
    def __deposit(self, amount):
        if amount > 0:
            self.__balance += amount
            print(f"Agregado {amount} al saldo")
        else:
            print("El monto del deposito debe ser positivo")

    def __withdraw(self, amount):
        if 0 < amount <= self.__balance:
            self.__balance -= amount
            print(f"Retirado {amount} del saldo")
        else:
            print("Retiro invalido")

    def __get_balance(self):
        return self.__balance

    # Metodos publicos
    def __repr__(self):
        return f"BankAccount -> (owner={self.__owner}, balance={self.__balance})"

    def process(self, operation, *args):  # uno de los metodos para llamar cada uno de los métodos de manera publica
        if operation in self.__operations:
            return self.__operations[operation](*args)
        else:
            print(f"Operación '{operation}' no disponible")
            return None


