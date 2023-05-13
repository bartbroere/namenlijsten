import random
import string

import pandas

from unidecode import unidecode
from collections import Counter


def count(x):
    if x == '< 5':
        return 2
    else:
        return int(x)

def numbers_only(x):
    try:
        return int(''.join(filter(str.isnumeric, x)))
    except:
        return None


familienamen = pandas.read_csv('familienamen-v2020.01.03.cleaned.csv')
familienamen['normalized'] = familienamen['achternaam'].\
    apply(unidecode).\
    apply(str.upper).\
    apply(lambda x: ''.join(filter(str.isalpha, x))).\
    apply(lambda x: x[:4])
familienamen['amount'] = familienamen['counts'].apply(count)
familienamen = familienamen.groupby('normalized').sum()
del familienamen['counts']
del familienamen['achternaam']
del familienamen['exactified']
familienamen = familienamen.sort_values('amount', ascending=False)

bevolkingsopbouw = pandas.read_csv('Leeftijdsopbouw Nederland 2023 (prognose).csv', sep=';')
bevolkingsopbouw['Leeftijd'] = bevolkingsopbouw['Leeftijd'].apply(numbers_only)
bevolkingsopbouw = bevolkingsopbouw.dropna()
bevolkingsopbouw['Mannen'] = bevolkingsopbouw['Mannen'].apply(numbers_only).apply(int)
bevolkingsopbouw['Vrouwen'] = bevolkingsopbouw['Vrouwen'].apply(numbers_only).apply(int)
bevolkingsopbouw['Som'] = bevolkingsopbouw['Mannen'] + bevolkingsopbouw['Vrouwen']
bevolkingsopbouw['Geboortejaar'] = 2023 - bevolkingsopbouw['Leeftijd']
bevolkingsopbouw['Laatste_twee'] = bevolkingsopbouw['Geboortejaar'].apply(int).apply(str).apply(lambda x: x[-2:])
bevolkingsopbouw = bevolkingsopbouw.groupby('Laatste_twee').sum()
bevolkingsopbouw = bevolkingsopbouw.sort_values('Som', ascending=False)

for voorletter in 

sample_size = sum(familienamen['amount'])

kenos = []
for four, first, two in zip(
        random.choices(list(familienamen.index), weights=list(familienamen['amount']), k=sample_size),
        random.choices(string.ascii_uppercase, k=sample_size),
        random.choices(list(bevolkingsopbouw.index), weights=list(bevolkingsopbouw['Som']), k=sample_size)
):
    kenos.append(f'{four}{first}{two}')

kenos = Counter(kenos).most_common()

ones = 0
mores = 0
for keno, how_many in kenos:
    if how_many == 1:
        ones += how_many
    else:
        mores += how_many

percentage = mores / (ones + mores) * 100.

print(percentage)


# -- keno_lang
kenos = []
for four, first, two, month, day in zip(
        random.choices(list(familienamen.index), weights=list(familienamen['amount']), k=sample_size),
        random.choices(string.ascii_uppercase, k=sample_size),
        random.choices(list(bevolkingsopbouw.index), weights=list(bevolkingsopbouw['Som']), k=sample_size),
        random.choices(['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'], k=sample_size),
        random.choices([str(x).zfill(2) for x in range(1, 31)], k=sample_size),
):
    kenos.append(f'{four}{first}{two}{month}{day}')

kenos = Counter(kenos).most_common()

ones = 0
mores = 0
for keno, how_many in kenos:
    if how_many == 1:
        ones += how_many
    else:
        mores += how_many

percentage = mores / (ones + mores) * 100.

print(percentage)
