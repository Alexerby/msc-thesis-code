from __future__ import annotations

"""Centralised SOEP‑loader registry.

This module hides every table‑name / column‑list detail behind a single
interface so that higher‑level code (e.g. `BafoegCalculator`) only speaks
in domain language (ppath, pl, …) and never touches I/O specifics again.
"""

from typing import Dict, List, Tuple
from data_handler import SOEPDataHandler

import pandas as pd

# ---------------------------------------------------------------------------
# Table specification
# ---------------------------------------------------------------------------
# key -> (filename in SOEP archive, list_of_columns_to_read)
# key: (filename, columns, config_section)
_SPEC: Dict[str, Tuple[str, List[str], str]] = {
    "ppath": (
        "ppathl",
        [
            "pid",
            "hid",
            "syear",
            "gebjahr",
            "sex",
            "gebmonat",
            "partner",
            "migback",
        ],
        "soep"
    ),
    "biosib": (
        "biosib",
        [
            "pid",
            "sibpnr1",
            "sibpnr2",
            "sibpnr3",
            "sibpnr4",
            "sibpnr5",
            "sibpnr6",
            "sibpnr7",
            "sibpnr8",
            "sibpnr9",
            "sibpnr10",
            "sibpnr11",
        ],
        "soep"
    ),
    "pl": (
        "pl",
        [
            "pid",
            "syear",
            "plg0012_h",
            "plh0258_h",
            "plc0167_h",
            "plc0168_h",
            "plg0014_v5",
            "plg0014_v6",
            "plg0014_v7",
        ],
        "soep"
    ),
    "pgen": (
        "pgen",
        [
            "pid",
            "syear",
            "pglabgro",
            "pgemplst",
            "pgpartnr",
            "pgisced11"
        ],
        "soep"
    ),
    "pkal": (
        "pkal",
        [
            "pid",
            "syear",
            "kal2a02",
            "kal2a03_h"
        ],
        "soep"
    ),
    "pwealth": (
        "pwealth",
        [
            "pid",
            "syear",
            "f0100a", "f0100b", "f0100c", "f0100d", "f0100e",
            "e0111a", "e0111b", "e0111c", "e0111d", "e0111e",
            "b0100a", "b0100b", "b0100c", "b0100d", "b0100e",
            "i0100a", "i0100b", "i0100c", "i0100d", "i0100e",
            "v0100a", "v0100b", "v0100c", "v0100d", "v0100e",
            "t0100a", "t0100b", "t0100c", "t0100d", "t0100e",
            "w0011a", "w0011b", "w0011c", "w0011d", "w0011e"
        ],
        "soep"
    ),
    "bioparen": (
        "bioparen",
        ["pid", "fnr", "mnr"],
        "soep"
    ),
    "region": (
        "regionl",
        ["hid", "bula", "syear"],
        "soep"
    ),
    "hgen": (
        "hgen",
        ["hid", "hgtyp1hh", "syear"],
        "soep"
    ),
    "pequiv": (
        "pequiv",
        ["pid", "istuy", "syear"],
        "soep"
    ),
    "biol": (
        "biol",
        ["pid", "syear", "lb0285", "lb0267_v1"],
        "soep"
    ),
    "phrf": (
        "phrf",
        [
            "pid",
            "aphrf", "bphrf", "cphrf", "dphrf", "ephrf", "fphrf", "gphrf", "hphrf", "iphrf",
            "jphrf", "kphrf", "lphrf", "mphrf", "nphrf", "ophrf", "pphrf", "qphrf", "rphrf",
            "sphrf", "tphrf", "uphrf", "vphrf", "wphrf", "xphrf", "yphrf", "zphrf",
            "baphrf", "bbphrf", "bcphrf", "bdphrf", "bephrf", "bfphrf", "bgphrf", "bhphrf",
            "biphrf", "bjphrf", "bkphrf", "blphrf"
        ],
        "soep_raw"
    )
}


class LoaderRegistry:
    """Lazy‑loading façade around all SOEP sheets we need.

    Example
    -------
    >>> loaders = LoaderRegistry()
    >>> df_students = loaders.ppath()       # loads only ppath on first call
    >>> loaders.load_all()                  # force all remaining sheets in one go
    """

    # Dynamically create one SOEPDataHandler per spec entry
    def __init__(self) -> None:
        self._handlers: Dict[str, SOEPDataHandler] = {
            key: SOEPDataHandler(filename, config_section=config_section)
            for key, (filename, _, config_section) in _SPEC.items()
        }
        self.data: Dict[str, "pd.DataFrame"] = {}

    # ---------------------------------------------------------------------
    # Generic helpers
    # ---------------------------------------------------------------------
    def load(self, key: str):
        if key in self.data:
            return self.data[key]
        filename, cols, config_section = _SPEC[key]
        handler = self._handlers[key]
        handler.load_dataset(cols)
        self.data[key] = handler.data.copy()
        return self.data[key]


    def load_all(self):
        """Eagerly load every sheet defined in the spec."""
        for key in _SPEC.keys():
            self.load(key)

    # ---------------------------------------------------------------------
    # Convenience getters (so callers don’t need magic strings)
    # ---------------------------------------------------------------------
    def ppath(self):
        # Core person-year panel tracking: age, sex, household ID, etc.
        return self.load("ppath")

    def bioparen(self):
        # Parent-child linkages: maps pid → (fnr, mnr)
        return self.load("bioparen")


    def biosib(self):
        # Long-format biography module: fertility, children, life events
        return self.load("biosib")

    # ---------------------------------------------------------------------
    # Long datasets (person-year observations)
    # ---------------------------------------------------------------------
    def pl(self):
        # Person-year survey data: education status, religion, BAföG amount
        return self.load("pl")


    def biol(self):
        # Long-format biography module: fertility, children, life events
        return self.load("biol")


    def region(self):
        # Regional info per household: state (bula), district codes, etc.
        return self.load("region")


    def pequiv(self):
        # Generated person-level transfer/income indicators (e.g., istuy)
        return self.load("pequiv")


    def pkal(self):
        # Generated person-level transfer/income indicators (e.g., istuy)
        return self.load("pkal")


    def pwealth(self):
        # Generated person-level transfer/income indicators (e.g., istuy)
        return self.load("pwealth")

    # ---------------------------------------------------------------------
    # Generated datasets (cleaned/derived variables)
    # ---------------------------------------------------------------------
    def hgen(self):
        # Household-level generated variables: type of household, etc.
        return self.load("hgen")


    def pgen(self):
        # Person-level generated variables: income, employment status
        return self.load("pgen")


    # ---------------------------------------------------------------------
    # Dunder methods for convenience / debugging
    # ---------------------------------------------------------------------
    def __getitem__(self, key: str):
        """Dictionary‑style access to already‑loaded sheets."""
        return self.data[key]

    def __contains__(self, key: str):
        return key in self.data

    def __iter__(self):
        return iter(self.data.items())
