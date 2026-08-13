"""O gerador do badge tambem e codigo: se ele quebrar, o CI publica um SVG invalido."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from coverage_badge import color_for, coverage_percent, render


@pytest.fixture
def coverage_xml(tmp_path):
    path = tmp_path / "coverage.xml"
    path.write_text('<?xml version="1.0" ?><coverage line-rate="0.9037" version="7.6"/>')
    return path


def test_reads_percentage_from_report(coverage_xml):
    assert coverage_percent(coverage_xml) == 90.4


@pytest.mark.parametrize(
    ("percent", "expected"),
    [(100.0, "#4c1"), (95.0, "#4c1"), (90.0, "#97ca00"), (80.0, "#a4a61d"),
     (65.0, "#dfb317"), (45.0, "#fe7d37"), (10.0, "#e05d44")],
)
def test_color_thresholds(percent, expected):
    assert color_for(percent) == expected


def test_renders_valid_svg():
    svg = render(90.4)
    root = ET.fromstring(svg)  # noqa: S314 - conteudo gerado por nos
    assert root.tag.endswith("svg")
    assert "90%" in svg
    assert root.attrib["height"] == "20"


def test_badge_is_self_contained():
    """Sem referencia externa: o badge tem que renderizar no README do GitHub,
    que bloqueia recursos de fora."""
    svg = render(75.0)
    assert "http://" not in svg.replace("http://www.w3.org/2000/svg", "")
    assert "<image" not in svg
