import string
from collections import namedtuple
from random import shuffle

import bs4
import pandas
import requests
import requests_cache
from tqdm import tqdm


def get_directory(offset, letter, treffers):
    return requests.get(f'https://www.cbgfamilienamen.nl/nfb/lijst_namen.php?offset={offset}&naam={letter}&treffers={treffers}&operator=bw')


def add_gemeenten(row):
    if row.link is not None:
        detail_page = requests.get(f'https://www.cbgfamilienamen.nl/nfb/{row.link}').text
        detail_page = bs4.BeautifulSoup(detail_page, features="html.parser")
        try:
            map_image = detail_page.select('img[usemap]')[0].get('src')
            requests.get(f'https://www.cbgfamilienamen.nl/{map_image}')
        except IndexError:
            pass
        # try:
        #     detail_page.select('script:not([async])')[0].text
        # except IndexError:
        #     pass


if __name__ == '__main__':
    requests_cache.install_cache('meertens')
    tqdm.pandas()

    letters = list(string.ascii_lowercase)
    shuffle(letters)

    AchternaamRecord = namedtuple("AchernaamRecord", ('achternaam', 'counts', 'link'))
    achternamen = []

    for letter in tqdm(letters):
        offset = 0
        treffers = int(
            requests.get(f'https://www.cbgfamilienamen.nl/nfb/lijst_namen.php?operator=bw&naam={letter}').text.split(
                "treffers=")[1].split("&")[0])
        while offset + 50 < treffers:
            offset += 50
            data = bs4.BeautifulSoup(get_directory(offset, letter, treffers).text, features="html.parser")

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
    achternamen.to_csv('achternamen.csv.gz', compression='gzip')
    achternamen.progress_apply(add_gemeenten, axis=1)
