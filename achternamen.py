import io
import os
import string
from collections import deque, Counter, namedtuple
from random import shuffle
from tempfile import TemporaryDirectory

import bs4
import numpy
import pandas
import rasterio
import requests
import requests_cache
from PIL import Image
from more_itertools import grouper
from pandarallel import pandarallel
from rasterio.mask import raster_geometry_mask
from shapely.geometry import Polygon
from slimit import ast
from slimit.parser import Parser
from slimit.visitors import nodevisitor
from tqdm import tqdm


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
    directory = TemporaryDirectory()
    if row.link is not None:
        detail_page = requests.get(f'https://www.cbgfamilienamen.nl/nfb/{row.link}').text
        detail_page = bs4.BeautifulSoup(detail_page, features="html.parser")
        try:
            row['gemeenten'] = parse_javascript(detail_page.select('script:not([async])')[0].text)
        except IndexError:
            pass
        try:
            rel_detail_page = detail_page.select('td.justification-left ~ td.justification-left > a')[0].get('href')
            rel_detail_page = requests.get(f'https://www.cbgfamilienamen.nl/nfb/{rel_detail_page}').text
            rel_detail_page = bs4.BeautifulSoup(rel_detail_page, features='html.parser')
            detail_pages = [detail_page, rel_detail_page]
        except IndexError:
            detail_pages = [detail_page]
        try:
            for detail_page in detail_pages:
                map_image = detail_page.select('img[usemap]')[0].get('src')
                url = f'https://www.cbgfamilienamen.nl/{map_image}'
                image_data = requests.get(url).content
                with open(os.path.join(directory.name, url.split("=")[-1]), 'wb') as w:
                    w.write(image_data)
                image = Image.open(io.BytesIO(image_data))
                raster = rasterio.open(os.path.join(directory.name, url.split("=")[-1]))
                gemeenten = {}
                for area in detail_page.select('map > area'):
                    points = deque(int(x) for x in area.get('coords').split(','))
                    gemeente_mask = raster_geometry_mask(raster, [Polygon(grouper(points, 2))], invert=True)
                    pixel_counter = Counter(numpy.array(image)[gemeente_mask[0]])
                    gemeenten[area.get('alt')] = pixel_counter
                os.unlink(os.path.join(directory.name, url.split("=")[-1]))
                if 'abs.png' in url:
                    row['abs_pixel_counters'] = gemeenten
                if 'rel.png' in url:
                    row['rel_pixel_counters'] = gemeenten
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

    for letter in tqdm(['broe']):
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

    achternamen = pandas.DataFrame(achternamen)
    achternamen = achternamen.progress_apply(add_gemeenten, axis=1)
    achternamen.to_csv('achternamen.csv.gz', compression='gzip')
    achternamen.to_csv('achternamen.csv')
