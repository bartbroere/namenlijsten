import hashlib
import string
from collections import namedtuple
from glob import glob
from random import shuffle

import bs4
import pandas
import requests
import requests_cache
from ratelimit import limits, sleep_and_retry
from tqdm import tqdm


@sleep_and_retry
@limits(calls=1, period=4)
def get_directory(offset, letter, treffers):
    return requests.get(f'https://www.cbgfamilienamen.nl/nfb/lijst_namen.php?offset={offset}&naam={letter}&treffers={treffers}&operator=bw')


def download_page(link):
    hashed_link = hashlib.md5()
    hashed_link.update(link.encode('utf8'))
    with open('data/' + hashed_link.hexdigest(), 'w') as w:
        w.write(requests.get(f'https://www.cbgfamilienamen.nl/nfb/{link}').text)


def add_gemeenten(row):
    if row.link is not None:
        hashed_link = hashlib.md5()
        hashed_link.update(row.link.encode('utf8'))
        with open('data/' + hashed_link.hexdigest(), 'r') as f:
            detail_page = bs4.BeautifulSoup(f.read())
        print(detail_page.find('script:not([async])')[0].text)


if __name__ == '__main__':
    requests_cache.install_cache('meertens')
    tqdm.pandas()

    letters = list(string.ascii_lowercase)
    shuffle(letters)

    for letter in tqdm(letters):
        offset = 0
        treffers = int(
            requests.get(f'https://www.cbgfamilienamen.nl/nfb/lijst_namen.php?operator=bw&naam={letter}').text.split(
                "treffers=")[1].split("&")[0])
        while offset + 50 < treffers:
            offset += 50
            data = get_directory(offset, letter, treffers)
            with open(f'data/{letter}-{offset}.html', 'w') as w:
                w.write(data.text)

    AchternaamRecord = namedtuple("AchernaamRecord", ('achternaam', 'counts', 'link'))
    achternamen = []

    for filename in glob('data/*-*.html'):
        data = bs4.BeautifulSoup(open(filename, 'r').read())

        for achternaam in data.select('td.justification-right'):
            tr = achternaam.parent
            fields = tr.select('td')
            achternaam = fields[0].text.strip()
            try:
                link = fields[0].select('a')[0].get('href')
            except:
                link = None
            counts = fields[1].text.strip()
            achternamen.append(AchternaamRecord(achternaam, counts, link))

    achternamen = pandas.DataFrame(achternamen)
    achternamen.to_csv('achternamen.csv')

    achternamen.link.dropna().progress_apply(download_page)
    achternamen.link.dropna().progress_apply(add_gemeenten)
