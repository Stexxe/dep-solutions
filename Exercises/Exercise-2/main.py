import os
import sys
import requests
import pandas
from html.parser import HTMLParser


# Since the Last-Modified date/time `2022-02-07 14:03` doesn't exist anymore,
# the script searches for the file by given via CLI year and Last-Modified date/time
def main():
    if len(sys.argv) < 2:
        print(f"Usage: {os.path.basename(sys.argv[0])} year last-modified", file=sys.stderr)
        sys.exit(1)

    year, last_modified = sys.argv[1], sys.argv[2]
    url = f"https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}"
    response = requests.get(url)

    parser = FilenameSearcherParser(last_modified)

    try:
        found = False
        parser.feed(response.text)
    except StopParsingException:
        found = True

    if not found:
        print(f"File with {last_modified} not found", file=sys.stderr)
        sys.exit(1)

    file_url = f"{url}/{parser.last_filename}"
    print(f"Found {file_url}")

    df = pandas.read_csv(file_url)
    col = df['HourlyDryBulbTemperature'].apply(normalize_float).astype('float64')
    rows = df.loc[col == col.max()]
    pandas.set_option('display.max_columns', None)

    print(rows)


def normalize_float(x):
    try:
        float(x)
        return x
    except ValueError:
        return '0'


class FilenameSearcherParser(HTMLParser):
    def __init__(self, last_modified):
        super().__init__()
        self.insideRow = False
        self.currentCol = 0
        self.last_modified = last_modified
        self.last_filename = ''

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.insideRow = True
            self.currentCol = -1
        elif tag == "td" and self.insideRow:
            self.currentCol += 1

    def handle_endtag(self, tag):
        if tag == "tr":
            self.insideRow = False

    def handle_data(self, data):
        if not self.insideRow:
            return

        if self.currentCol == 0:  # filename
            self.last_filename = data
        elif self.currentCol == 1:  # last-modified
            if self.last_modified == data.strip():
                raise StopParsingException()


class StopParsingException(Exception):
    pass


if __name__ == "__main__":
    main()
