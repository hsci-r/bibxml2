from typing import cast
import pytest
import lxml.etree
from bib2 import convert_picaxml_record, convert_marcxml_record
from unicodedata import normalize

# Helper to build XML elements
def make_elem(tag, attrib=None, text=None, children=None) -> lxml.etree._Element:
    attrib = attrib or {}
    elem = lxml.etree.Element(tag, attrib, None)
    if text is not None:
        elem.text = text
    if children:
        for child in children:
            elem.append(child)
    return elem

def test_convert_pica_record_basic():
    # <record><field tag="003@"><subfield code="0">123</subfield></field></record>
    subfield = make_elem('subfield', {'code': '0'}, '123')
    field = make_elem('field', {'tag': '003@'}, children=[subfield])
    record = [field]
    result = list(convert_picaxml_record(record))
    assert result == [(1, 1, '003@', '0', normalize('NFC', '123'))]

def test_convert_pica_record_empty_subfield():
    # Should skip subfields with no text
    subfield = make_elem('subfield', {'code': '0'})
    field = make_elem('field', {'tag': '003@'}, children=[subfield])
    record = [field]
    result = list(convert_picaxml_record(record))
    assert result == []

def test_convert_marc_record_leader_and_controlfield():
    leader = make_elem('{http://www.loc.gov/MARC21/slim}leader', text='leadertext')
    control = make_elem('{http://www.loc.gov/MARC21/slim}controlfield', {'tag': '001'}, 'ctrl')
    record = cast(lxml.etree._ElementIterator, [leader, control])
    result = list(convert_marcxml_record(record))
    assert result == [
        (1, 1, 'LDR', '', 'leadertext'),
        (2, 1, '001', '', 'ctrl')
    ]

def test_convert_marc_record_datafield():
    # Datafield with indicators and subfields
    sub1 = make_elem('subfield', {'code': 'a'}, 'foo')
    sub2 = make_elem('subfield', {'code': 'b'}, 'bar')
    field = make_elem('{http://www.loc.gov/MARC21/slim}datafield', {'tag': '245', 'ind1': '1', 'ind2': ' '}, children=[sub1, sub2])
    record = cast(lxml.etree._ElementIterator, [field])
    result = list(convert_marcxml_record(record))
    # ind1 yields a 'Y' subfield, ind2 is blank (skipped)
    assert result == [
        (1, 1, '245', 'Y', normalize('NFC', '1')),
        (1, 2, '245', 'a', normalize('NFC', 'foo')),
        (1, 3, '245', 'b', normalize('NFC', 'bar'))
    ]

def test_convert_marc_record_datafield_skip_empty_subfield():
    sub1 = make_elem('subfield', {'code': 'a'})
    field = make_elem('{http://www.loc.gov/MARC21/slim}datafield', {'tag': '100', 'ind1': ' ', 'ind2': ' '}, children=[sub1])
    record = cast(lxml.etree._ElementIterator, [field])
    result = list(convert_marcxml_record(record))
    assert result == []
