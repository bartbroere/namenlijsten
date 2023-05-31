import json
import os
import string

import more_itertools
import requests
from bs4 import BeautifulSoup
from ratelimit import limits, sleep_and_retry


@sleep_and_retry
@limits(calls=1, period=2)  # one call per two seconds
def parse_table(page):
    return list(more_itertools.chunked([x.text for x in page.find_all('td')][3:], 3))


for letter in string.ascii_lowercase:
    names = []
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


all_names = set()
for letter in string.ascii_lowercase:
    with open(f'{letter}.json', 'r') as f:
        data = [tuple(x) for x in json.loads(f.read())]
        all_names.update(data)
    os.unlink(f'{letter}.json')

with open('voornamen.json', 'w') as w:
    w.write(json.dumps(list(sorted(all_names))))
