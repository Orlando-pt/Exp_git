from Sigla import Sigla
from Voo import Voo
import pprint

siglas = {}
lista_voos = []
def lerCompanhias(nome_ficheiro):
    with open(nome_ficheiro) as f:
        for linha in f:
            if linha.startswith('Sigla'):
                continue
            s = Sigla(linha.rstrip())
            siglas[s.sigla] = s.companhia

def lerVoos(nome_ficheiro):
    with open(nome_ficheiro) as ficheiro:
        for linha in ficheiro:
            if linha.startswith('Hora'):
                continue
            v = Voo(linha.rstrip())
            if v.sigla in siglas:
                v.setCompanhia(siglas.get(v.sigla))
            else:
                v.setCompanhia("DESCONHECIDA")
            lista_voos.append(v)

def printListaVoos():
    print('{:<8s} {:<10s} {:<20s} {:<30s} {:<8s} {:<8s}'.format('Hora', 'Voo', 'Companhia', 'Origem', 'Atraso', 'Obs'))
    for voo in lista_voos:
        if voo.atraso != None:
            print('{:<8s} {:<10s} {:<20s} {:<30s} {:<8s} {:<8s}'.format(voo.hora, voo.voo, voo.companhia, voo.origem, voo.atraso, 'Previsto: ' + ':'.join(voo.getObs.split(":")[:2])))
        else:
            print('{:<8s} {:<10s} {:<20s} {:<30s} {:<8s} {:<8s}'.format(voo.hora, voo.voo, voo.companhia, voo.origem, ' ', ' '))
def main():
    lerCompanhias('companhias.txt')
    lerVoos('voos.txt')
    printListaVoos()

if __name__ == '__main__':
    main()