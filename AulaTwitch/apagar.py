class Voo:
    def __init__(self):
        self._hora = None
        self._nome = None
        self._origem = None
        self._atraso = None
        self._sigla = None
        self._numero = None

    def _split_flight_name(self, nome):
        sigla = ""
        numero = ""
        for c in nome:
            if c.isnumeric():
                number += c
            else:
                sigla += c
        return sigla.rstrip(), numero
    
    def parse_line(self, linha):
        try:
            hora, nome, origem, atraso = linha.split('\t')
        except ValueError:
            hora, nome, origem = linha.split('\t')
            atraso = None
​
        self._hora = hora
        self._sigla, self._numero = self._split_flight_name(nome)
        self._origem = origem
        self._atraso = atraso
    @property
    def hora(self):
        return self._hora
    @property
    def sigla(self):
        return self._sigla
    @property
    def origem(self):
        return self._origem

    @property
    def numero(self):
        return self._numero
    @property
    def atraso(self):
        return self._atraso

    def __str__(self):
        return f'{self._hora}\t{self._sigla}-{self._numero}\t\t\t{self._atraso}'

    def __repr__(self):
        return self.__str__()