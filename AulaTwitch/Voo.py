from datetime import timedelta

class Voo:
    def __init__(self, linha):
        l = linha.split("\t")
        self._hora = l[0]
        self._voo = l[1]
        self._origem = l[2]
        self._sigla = self._voo[:2]
        self._companhia = None
        try:
            if not l[3] in "\t\r\n":
                self._atraso = l[3]
                self._obs = self.obs(l[0], l[3])
            else:
                self._atraso = None
                self._obs = None
        except IndexError:
            self._atraso = None
            self._obs = None

    def setCompanhia(self, comp):
        self._companhia = comp

    def obs(self, hora, atraso):
        #tem de pegar na hora e somar atraso
        horas, minutos = hora.split(":")
        atraso_horas, atraso_minutos = atraso.split(":")
        return str(timedelta(hours=int(horas), minutes=int(minutos)) + timedelta(hours=int(atraso_horas), minutes=int(atraso_minutos)))


    @property
    def hora(self):
        return self._hora
    
    @property
    def voo(self):
        return self._voo

    @property
    def origem(self):
        return self._origem

    @property
    def atraso(self):
        return self._atraso

    @property
    def sigla(self):
        return self._sigla

    @property
    def companhia(self):
        return self._companhia

    @property
    def getObs(self):
        return self._obs

    def __str__(self):
        return f'{self._hora}\t{self._voo}\t{self._companhia}\t{self._origem}\t\t\t{self._atraso}'
    
    def __repr__(self):
        return self.__str__()