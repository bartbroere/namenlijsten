import random
import string

import pandas
from collections import defaultdict
import json

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

letter_distribution = defaultdict(int)
for voorletter in string.ascii_lowercase:
    with open(f'{voorletter}.json', 'r') as f:
        for name in json.load(f):
            if name[0][0].lower() == voorletter:
                for index in [1, 2]:
                    if name[index] == '< 5':
                        letter_distribution[voorletter] += 2
                    elif name[index] == '-':
                        ...
                    else:
                        letter_distribution[voorletter] += int(name[index])

voorletters = list(letter_distribution.items())
letter, letter_prevalence = [x[0] for x in voorletters], [x[1] for  x in voorletters]

print(sorted(voorletters, key=lambda x: -x[1]))

from matplotlib import pyplot

pyplot.figure()
pyplot.title("Eerste letter van de voornaam")
pyplot.barh(letter[::-1], letter_prevalence[::-1])
pyplot.savefig('voorletters.png')

pyplot.figure()
pyplot.title("Eerste vier letters van de achternaam")
pyplot.barh(list(familienamen.index)[0:25][::-1], list(familienamen['amount'])[0:25][::-1])
pyplot.savefig('achternamen.png')

pyplot.figure(figsize=(20, 8))
pyplot.title("Laatste twee cijfers van het geboortejaar")
pyplot.bar(list(bevolkingsopbouw.index), list(bevolkingsopbouw['Som']))
pyplot.xticks(rotation=90)
pyplot.savefig('jaren.png')

sample_size = sum(familienamen['amount'])

kenos = []
for four, first, two in zip(
        random.choices(list(familienamen.index), weights=list(familienamen['amount']), k=sample_size),
        random.choices(letter, weights=letter_prevalence, k=sample_size),
        random.choices(list(bevolkingsopbouw.index), weights=list(bevolkingsopbouw['Som']), k=sample_size)
):
    kenos.append(f'{four}{first}{two}')

kenos = Counter(kenos).most_common()

ones = 0
mores = 0
for keno, how_many in kenos:
    if keno == 'BROEB94':
        print(keno, how_many)
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
        random.choices(letter, weights=letter_prevalence, k=sample_size),
        random.choices(list(bevolkingsopbouw.index), weights=list(bevolkingsopbouw['Som']), k=sample_size),
        random.choices([str(x).zfill(2) for x in range(1, 13)], k=sample_size),
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
