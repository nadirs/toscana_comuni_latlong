#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Copyright (c) 2013, Nadir Sampaoli
# Some rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# * Redistributions of source code must retain the above copyright notice, this
#       list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.
# * The names of the contributors may not be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from __future__ import print_function

import csv, os, sys, urllib2, zipfile
from StringIO import StringIO
from xml.dom.minidom import parse as xmlparse
from preprocess_sira import (KEY_URL_FOR_CSV, KEY_ISTAT_VALUES,
                             KEY_ISTAT_VALUE, KEY_TEMP_DIR,
                             KEY_SQL_MODEL, KEY_SQL_OUTPUT)

try:
    __file__
except NameError:
    __file__ = sys.argv[0]

# USAGE
# planned usage
usage = """
Toscana Parser.

Usage:
    {fn} (guest|host) (shutdown|reboot) <nomehost>
    {fn} guest rename <nomehost> <nuovonome>
""".format(fn=__file__)

# CONSTANTS
# path
HERE_DIR = os.path.abspath(os.path.dirname(__file__))
SRC_CSV_DELIMITER = '\t'
# CSV stuff
ISTAT_MARKER = "__CODICE_ISTAT_"
PROJECTION = ['CODICEISTAT', 'SIGLAPROV', 'COMUNE', 'LOCALITA', 'INDIRIZZO',
        'LONG WGS84', 'LAT WGS84']
NEW_COLUMNS = ['codiceistat', 'siglaprov', 'comune', 'localita', 'indirizzo',
        'long', 'lat']

CAP_EXTENSION = 'cap'

# CAP source file
CAP_COMUNI_CSV = os.path.join(HERE_DIR, 'cap_comuni.csv')
# CAP columns
KEY_CAP_ISTATCODE = 'istat'
KEY_CAP_CAP = 'cap'
CAP_PROJECTION = ['Istat', 'CAP']
CAP_ALIASES = [KEY_CAP_ISTATCODE, KEY_CAP_CAP]


def main(args={}):
    log("Begin.")

    # parse config xml
    xml_config_path = args.setdefault('--config', "sorgenteSIRA.xml")
    xml_config_path = abspath_here_if_not(xml_config_path)
    config = parse_source_xml(xml_config_path)
    url_for_csv = config[KEY_URL_FOR_CSV]
    istat_codes = config[KEY_ISTAT_VALUES]
    temp_dir    = config[KEY_TEMP_DIR]
    mk_temp_dir(temp_dir)

    if not args.setdefault('--no-download', False):
        skip = args.setdefault('--skip', False)
        download_zips(url_for_csv, istat_codes, temp_dir, skip=skip)

    result_matrix = []
    temps = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith('.csv')]
    for fname in sorted(temps):
        # parse each csv file inside zip
        log("Parsing {}...".format(fname))
        result_matrix.extend(parse_csv_file(fname)) #parse_csv_files_in_zip(zipfile.ZipFile(fname)))
        log("Done.")

    # if cap flag is set join CAP to main matrix
    if config.setdefault('--cap', CAP_COMUNI_CSV):
        log("Joining CAP column to matrix...")
        join_cap_to_matrix_by_istat(config['--cap'], result_matrix)
        log("Done.")

    if args['--csv']:
        csv_output_path = args['--csv']
        log("Writing CSV to output...")
        with open(csv_output_path, 'wb+') as outfile:
            csv_writer = csv.DictWriter(outfile, NEW_COLUMNS, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writeheader()
            csv_writer.writerows(result_matrix)
        log("Done.")

    if not args.setdefault('--no-sql', False):
        sql_model = config[KEY_SQL_MODEL]
        log("Writing SQL model '{}'...".format(sql_model))
        sql_string = format_sql(sql_model, result_matrix)
        with open(config[KEY_SQL_OUTPUT], 'wb+') as outsql:
            outsql.write(sql_string)
        log("Done.")

    log("Bye Bye.\n")

def log(s=''):
    print(s)

def alert(s):
    bord = "="*len(s)
    print("{}\n{}\n{}".format(bord, s))

def mk_temp_dir(temp_dir):
    try:
        os.mkdir(temp_dir)
    except OSError as e:
        log(e)

def parse_source_xml(src):
    def getElemValue(root, tag):
        return root.getElementsByTagName(tag)[0].childNodes[0].nodeValue

    sira_xml = xmlparse(src)
    root_node = sira_xml.childNodes[0]

    config = {}
    # url template
    config[KEY_URL_FOR_CSV] = getElemValue(root_node, KEY_URL_FOR_CSV)

    # istat codes
    items_node = root_node.getElementsByTagName(KEY_ISTAT_VALUE)
    config[KEY_ISTAT_VALUES] = [item.childNodes[0].nodeValue for item in items_node]

    # where to save downloads
    tmp_dir = getElemValue(root_node, KEY_TEMP_DIR)
    config[KEY_TEMP_DIR] = abspath_here_if_not(tmp_dir)

    # sql model file name
    sql_model = getElemValue(root_node, KEY_SQL_MODEL)
    config[KEY_SQL_MODEL] = abspath_here_if_not(sql_model)

    # sql output file name
    sql_output = getElemValue(root_node, KEY_SQL_OUTPUT)
    config[KEY_SQL_OUTPUT] = abspath_here_if_not(sql_output)

    return config

def abspath_here_if_not(path):
    if os.path.isabs(path):
        return path
    else:
        return os.path.join(HERE_DIR, path)

def download_zips(url_model, istat_codes, temp_dir, skip=False):
    for code in istat_codes:
        if skip and csv_already_downloaded(code, temp_dir):
            log("Skipped downloading {} according to skip flag".format(code))
            continue
        log("Downloading {}...".format(code))
        # curl each url passing code in GET request
        try:
            curl_with_istat_code(url_model, code, func=lambda zipf: save_zip_temp(zipf, code, temp_dir))
        except KeyboardInterrupt:
            log()
            log("Skipped downloading. Only .csv files found in '{temp_dir}' will be loaded".format(temp_dir=temp_dir))
            break
        log("Done.")

def csv_already_downloaded(code, temp_dir):
    return os.path.basename(tmp_csv_file(code, temp_dir)) in os.listdir(temp_dir)

def save_zip_temp(zipf, code, temp_dir):
    zipf = read_as_zip(zipf)
    temp_csvs = zipf.namelist()
    if len(temp_csvs) > 1:
        log("ALERT: zip {} contains more than one csv. Only the last one will be used!")
    for i, fname in enumerate(temp_csvs):
        with zipf.open(fname, 'rU') as infile:
            with open(tmp_csv_file(code, temp_dir), 'wb+') as outfile:
                outfile.write(infile.read())

def tmp_csv_file(code, temp_dir, suffix=''):
    return os.path.join(temp_dir, "tmp_{}{}.csv".format(code, suffix))

def read_as_zip(filestream):
    return zipfile.ZipFile(StringIO(filestream.read()))

def curl_with_istat_code(url, code, func=None):
    """
    Calls curl pre-formatting url
    """
    return curl(url.replace(ISTAT_MARKER, code), func=func)

def curl(url, func=None):
    """
    Download the content retrieved by the specified url or execute an optional
    function that takes the opened url stream as argument.
    """
    usock = urllib2.urlopen(url)
    try:
        if func:
            result = func(usock)
        else:
            result = usock.read()
    finally:
        usock.close()
    return result

def parse_csv_file(fname):
    """
    Opens file as a CSV and creates a matrix that is a
    concatenation of the projection
    """
    result = []
    with open(fname, 'rU') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=SRC_CSV_DELIMITER)
        result.extend(parse_csv(csv_reader))
    return result

def parse_csv(csv_reader, projection=PROJECTION, alias_columns=NEW_COLUMNS):
        """
        Returns a projection of the CSV's columns
        """
        result = []
        head_columns = skip_head(csv_reader)
        for row in csv_reader:
            result.append(projection_from_csv_row(row, head_columns, projection, alias_columns))
        return result

def skip_head(csv_reader):
    for headerline in csv_reader:
        if len(headerline) > 1:
            return headerline

def projection_from_csv_row(row, head_columns, projection, alias_columns):
    result = {}
    for i, column in enumerate(projection):
        value = row[head_columns.index(column)]
        result[alias_columns[i]] = "" if value == "-" else value.replace("\'", "\\'")
    return result

def format_sql(sqlmodel, data):
    s = []
    str_model = open(sqlmodel, 'rbU').read()
    for row in data:
        s.append(str_model.format(**row))
    return '\n'.join(s)

# addition: CAP join
def join_cap_to_matrix_by_istat(cap_csv_fname, main_matrix):
    istat_cap_pairs = pair_istatcode_cap(cap_csv_fname)
    caps_by_istat = {entry[KEY_CAP_ISTATCODE]: entry[KEY_CAP_CAP] for entry in istat_cap_pairs}

    # join CAP by (relational key is istat code)
    for row in main_matrix:
        try:
            row[CAP_EXTENSION] = caps_by_istat[row['codiceistat']]
        except KeyError:
            alert("[ABORT]: could NOT find CAP for ISTAT {}!!!".format(row['codiceistat']))


def pair_istatcode_cap(csv_fname):
    log("cap csv: {}".format(csv_fname))
    with open(csv_fname, 'rU') as f:
        csv_reader = csv.reader(f, delimiter=';')
        original_columns = skip_head(csv_reader)

        pair_matrix = []
        for row in csv_reader:
            pair_matrix.append(projection_from_csv_row(row, original_columns, CAP_PROJECTION, CAP_ALIASES))
        return pair_matrix

if __name__ == '__main__':
    import sys
    import docopt
    args = docopt.docopt(usage, argv=sys.argv[1:])
    print(args)
    sys.exit(0)

    if not args.setdefault('--verbose', False):
        del log
        def log(*args):
            pass

    main(args)
