import string
from collections import namedtuple, deque
from random import shuffle

import bs4
import pandas
import requests
import requests_cache
from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor
from tqdm import tqdm
from pandarallel import pandarallel


def get_directory(offset, letter, treffers):
    return requests.get(f'https://www.cbgfamilienamen.nl/nfb/lijst_namen.php?offset={offset}&naam={letter}&treffers={treffers}&operator=bw')


def parse_javascript(javascript_code):
    parser = Parser()
    tree = parser.parse(javascript_code)
    strings_in_javascript = deque(x.value for x in nodevisitor.visit(tree) if isinstance(x, ast.String))
    councils = {}
    if "'GoogleAnalyticsObject'" not in strings_in_javascript:
        while strings_in_javascript:
            key = strings_in_javascript.popleft()
            value = int(strings_in_javascript.popleft().replace(f"{key[:-1]} (", '').replace(")'", ""))
            councils[key[1:-1]] = value
    return councils


def add_gemeenten(row):
    if row.link is not None:
        detail_page = requests.get(f'https://www.cbgfamilienamen.nl/nfb/{row.link}').text
        detail_page = bs4.BeautifulSoup(detail_page, features="html.parser")
        try:
            map_image = detail_page.select('img[usemap]')[0].get('src')
            requests.get(f'https://www.cbgfamilienamen.nl/{map_image}')
        except IndexError:
            pass
        try:
            rel_detail_page = detail_page.select('td.justification-left ~ td.justification-left > a')[0].get('href')
            rel_detail_page = requests.get(f'https://www.cbgfamilienamen.nl/nfb/{rel_detail_page}').text
            rel_detail_page = bs4.BeautifulSoup(rel_detail_page, features='html.parser')
            rel_map_image = rel_detail_page.select('img[usemap]')[0].get('src')
            requests.get(f'https://www.cbgfamilienamen.nl/{rel_map_image}')
        except:
            pass
        # TODO turned off, this needs to be done in parallel
        try:
            row['gemeenten'] = parse_javascript(detail_page.select('script:not([async])')[0].text)
        except IndexError:
            pass
    return row


if __name__ == '__main__':
    requests_cache.install_cache('meertens')
    tqdm.pandas()
    pandarallel.initialize(progress_bar=True)

    letters = list(string.ascii_lowercase)
    shuffle(letters)

    AchternaamRecord = namedtuple("AchernaamRecord", ('achternaam', 'counts', 'link'))
    achternamen = []
    # letters = ['be']

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
    achternamen.parallel_apply(add_gemeenten, axis=1)
    achternamen.to_csv('achternamen.csv.gz', compression='gzip')
    achternamen.to_csv('achternamen.csv')
