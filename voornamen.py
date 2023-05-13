import json
import string
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import more_itertools
from ratelimit import limits, sleep_and_retry


@sleep_and_retry
@limits(calls=1, period=2)  # one call per two seconds
def parse_table(page):
    return list(more_itertools.chunked([x.text for x in page.find_all('td')][3:], 3))


names = []
for letter in 'bcdefghijklmnoprstuvwxyz':
    i = 1
    results = True

    while results:
        index = BeautifulSoup(requests.get(f'https://www.meertens.knaw.nl/nvb/naam/pagina{i}/begintmet/{letter}').text,
                              features='html.parser')
        results = parse_table(index)
        names.extend(results)
        i += 1

    with open(f'{letter}.json', 'w') as w:
        w.write(json.dumps(names))



print()