"""Gera o SVG do badge de cobertura a partir do coverage.xml.

Existe para o badge nao depender de servico de terceiro (Codecov e afins): o CI
gera o SVG e publica num branch `badges`. Menos uma conta externa no caminho, e o
badge fica sob o mesmo controle de acesso do repositorio.
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# verde forte -> vermelho, nos mesmos cortes que o shields.io usa
_THRESHOLDS = [
    (95, "#4c1"),
    (90, "#97ca00"),
    (75, "#a4a61d"),
    (60, "#dfb317"),
    (40, "#fe7d37"),
    (0, "#e05d44"),
]

_TEMPLATE = """<svg xmlns="http://www.w3.org/2000/svg" width="{total}" height="20" \
role="img" aria-label="coverage: {label}">
  <title>coverage: {label}</title>
  <linearGradient id="s" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
    <stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <clipPath id="r"><rect width="{total}" height="20" rx="3" fill="#fff"/></clipPath>
  <g clip-path="url(#r)">
    <rect width="{left}" height="20" fill="#555"/>
    <rect x="{left}" width="{right}" height="20" fill="{color}"/>
    <rect width="{total}" height="20" fill="url(#s)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" \
font-size="110" text-rendering="geometricPrecision">
    <text x="{left_x}" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" \
textLength="{left_text}">coverage</text>
    <text x="{left_x}" y="140" transform="scale(.1)" textLength="{left_text}">coverage</text>
    <text x="{right_x}" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" \
textLength="{right_text}">{label}</text>
    <text x="{right_x}" y="140" transform="scale(.1)" textLength="{right_text}">{label}</text>
  </g>
</svg>
"""


def coverage_percent(coverage_xml: Path) -> float:
    """Le a taxa de linhas cobertas do relatorio XML do coverage.py."""
    root = ET.parse(coverage_xml).getroot()  # noqa: S314 - arquivo gerado pelo proprio CI
    return round(float(root.attrib["line-rate"]) * 100, 1)


def color_for(percent: float) -> str:
    return next(color for threshold, color in _THRESHOLDS if percent >= threshold)


def render(percent: float) -> str:
    label = f"{percent:.0f}%"
    left, right = 62, 8 + len(label) * 8
    return _TEMPLATE.format(
        total=left + right, left=left, right=right, color=color_for(percent), label=label,
        left_x=left * 5, left_text=(left - 10) * 10,
        right_x=(left + right / 2) * 10, right_text=(right - 10) * 10,
    )


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("uso: coverage_badge.py <coverage.xml> <saida.svg>", file=sys.stderr)
        return 2
    percent = coverage_percent(Path(argv[1]))
    Path(argv[2]).write_text(render(percent), encoding="utf-8")
    print(f"cobertura {percent}% -> {argv[2]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
