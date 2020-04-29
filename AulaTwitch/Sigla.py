class Sigla:
    _sigla = ""
    _companhia = ""

    def __init__(self, linha):
        self._sigla, self._companhia = linha.split("\t")

    @property
    def sigla(self):
        return self._sigla

    @property
    def companhia(self):
        return self._companhia