from __future__ import annotations

"""Centralised SOEP‑loader registry.

This module hides every table‑name / column‑list detail behind a single
interface so that higher‑level code (e.g. `BafoegCalculator`) only speaks
in domain language (ppath, pl, …) and never touches I/O specifics again.
"""

from typing import Dict, List, Tuple

from data_handler import SOEPDataHandler

# ---------------------------------------------------------------------------
# Table specification
# ---------------------------------------------------------------------------
# key -> (filename in SOEP archive, list_of_columns_to_read)
_SPEC: Dict[str, Tuple[str, List[str]]] = {
    "ppath": (
        "ppathl",
        [
            "pid",
            "hid",
            "syear",
            "gebjahr",
            "sex",
            "gebmonat",
            # "parid",
            "partner",
            "migback",
        ],
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
    ),
    "pl": (
        "pl",
        # plg0012_h: Currently in education 
        # plh0258_h: Kirche, religion
        # plc0168_h: Bafoeg, Stipendium Bruttobetrag pro Monat
        [
         "pid", 
         "syear", 
         "plg0012_h", 
         "plh0258_h", 
         "plc0167_h", 
         "plc0168_h", 
         "plg0014_v5",  
         "plg0014_v6",  
         "plg0014_v7"
         ],
    ),
    "pgen": (
        "pgen",
        [
         "pid", 
         "syear", 
         "pglabgro", 
         "pgemplst", 
         "pgpartnr"
         ], 
    ),
    "pkal": (
        "pkal",
        [
         "pid", 
         "syear", 

         "kal2a02",
         "kal2a03_h"
         ], 
    ),

    "pwealth": (
        "pwealth",
        [
            "pid",
            "syear",

            # Financial Assets
            "f0100a", "f0100b", "f0100c", "f0100d", "f0100e",

            # Other Real Estate (Share Net Value)
            "e0111a", "e0111b", "e0111c", "e0111d", "e0111e",

            # Business Assets
            "b0100a", "b0100b", "b0100c", "b0100d", "b0100e",

            # Private Insurances (Building loan and insurances)
            "i0100a", "i0100b", "i0100c", "i0100d", "i0100e",

            # Vehicles
            "v0100a", "v0100b", "v0100c", "v0100d", "v0100e",

            # Tangible Assets
            "t0100a", "t0100b", "t0100c", "t0100d", "t0100e",

            # Overall Debts (excluding student loans)
            "w0011a", "w0011b", "w0011c", "w0011d", "w0011e"
        ],
    ),
    "bioparen": (
        "bioparen",
        ["pid", "fnr", "mnr"],
    ),
    "region": (
        "regionl",
        ["hid", "bula", "syear"],
    ),
    "hgen": (
        "hgen",
        ["hid", "hgtyp1hh", "syear"],
    ),
    "pequiv": (
        "pequiv",
        # istuy: Student grants
        ["pid", "istuy", "syear"]
    ),

    "biol": (
        "biol",
        ["pid", "syear", "lb0285"]
    ),
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
            key: SOEPDataHandler(filename) for key, (filename, _) in _SPEC.items()
        }
        # Cache for already‑loaded DataFrames
        self.data: Dict[str, "pd.DataFrame"] = {}

    # ---------------------------------------------------------------------
    # Generic helpers
    # ---------------------------------------------------------------------
    def load(self, key: str):
        """Load *one* sheet and return a fresh DataFrame copy."""
        if key in self.data:  # already loaded → return cached copy
            return self.data[key]

        filename, cols = _SPEC[key]
        handler = self._handlers[key]
        handler.load_dataset(cols)
        # Always store a copy so caller mutations don’t corrupt registry state
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
