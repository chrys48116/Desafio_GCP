texto = '01/01/01(loja)'
data, unidade = texto.split('(')
unidade = unidade.replace(')', '')
print(unidade)