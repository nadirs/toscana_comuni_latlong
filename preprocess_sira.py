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

# preprocess SIRA source

"""
XML structure:
    root
        urls
            url
            url
        items
            item
            item
            ...
            item
"""

KEY_URLS = 'urls'
KEY_URL_FOR_XML = 'url_for_xml'
KEY_URL_FOR_CSV = 'url_for_csv'
KEY_ISTAT_VALUES = 'items'
KEY_ISTAT_VALUE = 'item'
KEY_TEMP_DIR = 'directory'
KEY_SQL_MODEL = 'sqlmodel'
KEY_SQL_OUTPUT = 'sqloutput'

def escape(s):
    return (s.replace('&', '&amp;')
             .replace('<', '&lt;')
             .replace('>', '&gt;')
             .replace('"', '&quot;')
             .replace("'", '&#39;'))

def xml_elem_for(tagname, value, sub_list_tagname=KEY_ISTAT_VALUE):
    if type(value) is list:
        xml_for_list = ''.join([xml_elem_for(sub_list_tagname, list_entry) for list_entry in value])
        return xml_elem_for(tagname, xml_for_list)
    result = '<{tag}>{val}</{tag}>\n'.format(tag=tagname, val=value)
    return result

def xml_elem_for_dict_entry(d, k):
    entry = d[k]
    entry_type = type(entry)
    if entry_type is list:
        entry = map(lambda x:escape(str(x)), entry)
    else:
        entry = escape(unicode(entry))

    return xml_elem_for(k, entry)

def serialize_txt(in_fname):
    """
    Utility method for converting txt file to serialized file
    """
    txt_dict = {}
    with open(in_fname, 'rU') as infile:
        txt_urls, istat_lines = infile.read().split('\n\n')[::1]
        txt_dict[KEY_URL_FOR_XML], txt_dict[KEY_URL_FOR_CSV] = txt_urls.split('\n')
        txt_dict[KEY_ISTAT_VALUES] = istat_lines.strip('\n').split('\n')

    serialized_string = ''
    serialized_string += xml_elem_for(KEY_URLS, xml_elem_for_dict_entry(txt_dict, KEY_URL_FOR_XML)
                                               + xml_elem_for_dict_entry(txt_dict, KEY_URL_FOR_CSV))
    serialized_string += xml_elem_for_dict_entry(txt_dict, KEY_ISTAT_VALUES)

    return serialized_string

def main(out_fname):
    serialized_txt = '<?xml version="1.0" encoding="utf-8"?><root>\n'
    serialized_txt += xml_elem_for(KEY_TEMP_DIR, 'tmp')
    serialized_txt += xml_elem_for(KEY_SQL_MODEL, 'template.sql')
    serialized_txt += xml_elem_for(KEY_SQL_OUTPUT, 'output.sql')
    serialized_txt += serialize_txt(SIRA_SRC)
    serialized_txt += '</root>'

    with open(out_fname, 'w+') as outfile:
        outfile.write(serialized_txt)

if __name__ == '__main__':
    import os
    HERE_DIR = os.path.dirname(__file__)
    SIRA_SRC = os.path.join(HERE_DIR, "sorgenteSIRA.txt")
    CONFIG_XML = os.path.splitext(SIRA_SRC)[0] + '.xml'
    main(CONFIG_XML)
