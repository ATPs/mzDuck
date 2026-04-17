"""Helpers for locating bundled mzDuck example data."""

from __future__ import annotations

from importlib import resources


def example_data_path(filename: str):
    """Return a traversable path to a bundled example data file.

    Examples
    --------
    >>> from mzduck import example_data_path
    >>> tiny = example_data_path("tiny.mzduck")
    """
    return resources.files("mzduck").joinpath("example_data", filename)
