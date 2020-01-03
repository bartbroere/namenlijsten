import json
import os
import string
from collections import deque, Counter, namedtuple
from functools import lru_cache
from random import shuffle
from tempfile import TemporaryDirectory

import bs4
import dask.dataframe
import imageio
import pandas
import rasterio
import requests
import requests_cache
import webcolors
from distributed import Client, LocalCluster
from more_itertools import grouper
from rasterio.mask import raster_geometry_mask
from shapely.geometry import Polygon
from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor

ABSOLUTE_COLOR_MAPPING = {
    '#ffffff': (0, 0),
    '#ffeeaa': (1, 4),
    '#ffe680': (5, 10),
    '#ffdd55': (11, 25),
    '#ffd42a': (26, 50),
    '#ffcc00': (51, 100),
    '#d4aa00': (101, 250),
    '#aa8800': (251, 500),
    '#806600': (501, 1000),
    '#554400': (1001, 2500),
    '#2b2200': (2500,),
}

RELATIVE_COLOR_MAPPING = {
    '#ffffff': (0, 0),
    '#ffeeaa': (0, 0.01),
    '#ffe680': (0.01, 0.02),
    '#ffdd55': (0.02, 0.05),
    '#ffd42a': (0.05, 0.1),
    '#ffcc00': (0.1, 0.2),
    '#d4aa00': (0.2, 0.5),
    '#aa8800': (0.5, 1),
    '#806600': (1, 2),
    '#554400': (2, 5),
    '#2b2200': (5,),
}

AchternaamRecord = namedtuple("AchternaamRecord", ('achternaam', 'counts', 'link'))


@lru_cache(16)
def rgb_to_hex(rgb):
    return webcolors.rgb_to_hex(rgb)


def get_directory(offset, letter, treffers):
    return requests.get(
        f'https://www.cbgfamilienamen.nl/nfb/lijst_namen.php?offset={offset}&naam={letter}&treffers={treffers}&operator=bw')


def parse_javascript(javascript_code, as_type=int):
    parser = Parser()
    tree = parser.parse(javascript_code)
    strings_in_javascript = deque(x.value for x in nodevisitor.visit(tree) if isinstance(x, ast.String))
    councils = {}
    if "'GoogleAnalyticsObject'" not in strings_in_javascript:
        while strings_in_javascript:
            key = strings_in_javascript.popleft()
            value = as_type(strings_in_javascript.
                            popleft().
                            replace(f"{key[:-1]} (", '').
                            replace(")'", "").
                            replace(",", ".").
                            replace("%", ""))
            councils[key[1:-1]] = value
    return councils


def add_gemeenten(row):
    requests_cache.install_cache('meertens')
    directory = TemporaryDirectory()
    if row.link is not None:
        detail_page = requests.get(f'https://www.cbgfamilienamen.nl/nfb/{row.link}').text
        detail_page = bs4.BeautifulSoup(detail_page, features="html.parser")
        try:
            row['abs_gemeenten'] = json.dumps(parse_javascript(detail_page.select('script:not([async])')[0].text,
                                                               as_type=int))
        except IndexError:
            pass
        try:
            rel_detail_page = detail_page.select('td.justification-left ~ td.justification-left > a')[0].get('href')
            rel_detail_page = requests.get(f'https://www.cbgfamilienamen.nl/nfb/{rel_detail_page}').text
            rel_detail_page = bs4.BeautifulSoup(rel_detail_page, features='html.parser')
            try:
                row['rel_gemeenten'] = json.dumps(
                    parse_javascript(rel_detail_page.select('script:not([async])')[0].text,
                                     as_type=float))
            except IndexError:
                pass
            detail_pages = [detail_page, rel_detail_page]
        except IndexError:
            detail_pages = [detail_page]
        try:
            for detail_page in detail_pages:
                try:
                    map_image = detail_page.select('img[usemap]')[0].get('src')
                    url = f'https://www.cbgfamilienamen.nl/{map_image}'
                    image_data = requests.get(url).content
                    with open(os.path.join(directory.name, url.split("=")[-1]), 'wb') as w:
                        w.write(image_data)
                    image = imageio.imread(os.path.join(directory.name, url.split("=")[-1]), pilmode='RGB')
                    raster = rasterio.open(os.path.join(directory.name, url.split("=")[-1]))
                    gemeenten = {}
                    for area in detail_page.select('map > area'):
                        points = deque(int(x) for x in area.get('coords').split(','))
                        gemeente_mask = raster_geometry_mask(raster, [Polygon(grouper(points, 2))], invert=True)
                        pixel_counter = Counter(filter(
                            lambda x: x != '#808080' and x in ABSOLUTE_COLOR_MAPPING.keys(),
                            (rgb_to_hex(tuple(x)) for x in image[gemeente_mask[0]])))
                        if pixel_counter and 'abs.png' in url:
                            gemeenten[area.get('alt')] = ABSOLUTE_COLOR_MAPPING[pixel_counter.most_common(1)[0][0]]
                        if pixel_counter and 'rel.png' in url:
                            gemeenten[area.get('alt')] = RELATIVE_COLOR_MAPPING[pixel_counter.most_common(1)[0][0]]
                    os.unlink(os.path.join(directory.name, url.split("=")[-1]))
                    if 'abs.png' in url:
                        row['abs_pixel_counters'] = json.dumps(gemeenten)
                    if 'rel.png' in url:
                        row['rel_pixel_counters'] = json.dumps(gemeenten)
                except ValueError:
                    continue
        except IndexError:
            pass
    return row


def exactify(row):
    exactified = {}
    try:
        if row['counts'] != "< 5":
            assignable = int(row['counts'].replace(".", "")) - sum(int(x) for x in json.loads(row['abs_gemeenten']).values())
            abs_pixel_counters = {k: v for k, v in json.loads(row['abs_pixel_counters']).items() if v != [0, 0]}
            for exact_gemeente, value in json.loads(row['abs_gemeenten']).items():
                del abs_pixel_counters[exact_gemeente.replace("\\", "")]
                exactified[exact_gemeente.replace("\\", "")] = [value, value]
            for k, v in abs_pixel_counters.items():
                assignable -= v[0]
                exactified[k] = [v[0]]
            for k, v in abs_pixel_counters.items():
                exactified[k].append(min(exactified[k][0] + assignable, abs_pixel_counters[k][1]))
            row['exactified'] = json.dumps(exactified)
    except TypeError:
        pass
    except ValueError:
        pass
    row['counts'] = row['counts'].replace(".", "")
    return row


def get_index_of_letter(letter):
    requests_cache.install_cache('meertens')
    achternamen = []
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
            except IndexError:
                link = None
            counts = fields[1].text.strip()
            achternamen.append(AchternaamRecord(achternaam, counts, link))
    return achternamen


if __name__ == '__main__':
    requests_cache.install_cache('meertens')

    letters = list(string.ascii_lowercase)
    shuffle(letters)

    cluster = LocalCluster(n_workers=64, threads_per_worker=1)
    client = Client(cluster)

    # TODO change code block below to keep data in the Dask cluster, instead of up and down to the local context
    all_letters = client.map(get_index_of_letter, letters)
    achternamen = []
    for letter in client.gather(all_letters):
        for achternaam in letter:
            achternamen.append(achternaam)

    achternamen = pandas.DataFrame(achternamen).drop_duplicates()  # reduces from 406158 to 322292 entries
    achternamen = dask.dataframe.from_pandas(achternamen, npartitions=8192)
    achternamen['abs_gemeenten'] = ''
    achternamen['rel_gemeenten'] = ''
    achternamen['abs_pixel_counters'] = ''
    achternamen['rel_pixel_counters'] = ''
    achternamen['exactified'] = ''
    achternamen = achternamen.apply(add_gemeenten, axis=1)
    achternamen = achternamen.apply(exactify, axis=1).compute()
    # meta={'achternaam': 'object', 'counts': 'object', 'link': 'object', 'abs_pixel_counters': 'object', 'gemeenten': 'object', 'rel_pixel_counters': 'object'}

    # TODO use the combination of absolute and relative counts to estimate the total resident count for each municipality

    # TODO check the existence of the directory ./data/, and make if not there
    achternamen.to_csv('./data/achternamen.csv.gz', compression='gzip')
    achternamen.to_csv('./data/achternamen.csv')
