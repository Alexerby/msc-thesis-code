import math
import pandas as pd
from data_handler import SOEPStatutoryInputs

class TaxService:
    def __init__(self, gross_income_col: str = "gross_income"):
        self.gross_income_col = gross_income_col
        self.tax_table = self._load_tax_table()
        self.soli_table = self._load_soli_thresholds()
        self._invalid = {-1, -2, -3, -4, -5, -6, -7, -8}

    def compute_for_row(self, row: pd.Series) -> tuple[float | None, float | None, float | None]:
        """
        Compute income tax, church tax, and solidarity surcharge for a single row.
        Uses the column provided via the constructor as the taxable income source.
        """
        year = int(row["syear"])
        income = row.get(self.gross_income_col)

        itax = self._income_tax(income, year)
        if itax is None:
            return None, None, None

        # Church tax logic (optional rework)
        church = (
            math.floor(itax * (0.08 if row.get("bula") in [1, 2] else 0.09))
            if row.get("plh0258_h") in {1, 2} | self._invalid else 0
        )

        soli = self._soli(itax, row.get("hgtyp1hh"), year)
        return itax, church, soli

    # ---------- internal helpers ----------

    def _load_tax_table(self) -> pd.DataFrame:
        tb = SOEPStatutoryInputs("Income Tax")
        tb.load_dataset(columns=lambda _: True)

        df = tb.data.copy()

        # -- 1) normalise the *column names* -------------------------------
        df = df.rename(columns=lambda col: (
            col.replace("\xa0", " ")   # non‑breaking space → normal space
               .replace("\u200b", "")  # zero‑width space → drop
               .strip()                # trim outer whitespace
        ))

        # -- 2) convert string numbers like "9,400" → 9400.0 ---------------
        # df = df.apply(
        #     lambda col: pd.to_numeric(col.str.replace(",", "."), errors="ignore")
        #     if col.dtype == "object" else col
        # )


        df = df.apply(
            lambda col: (
                pd.to_numeric(col.str.replace(",", "."), errors="coerce")
                if col.dtype == "object" and col.str.contains(",", na=False).any()
                else col
            )
        )

        # -- 3 … 6)  your reindex / interpolate steps  ---------------------
        df["year"] = df["year"].astype(int)
        df = df.set_index("year").reindex(range(2002, 2026))
        num_cols = df.select_dtypes("number").columns
        df[num_cols] = df[num_cols].interpolate(method="linear",
                                                limit_direction="backward")
        df = df.reset_index()
        return df

    def _load_soli_thresholds(self):
        soli = SOEPStatutoryInputs("Solidaritätszuschlag")
        soli.load_dataset(columns=lambda _: True)
        df = soli.data.rename(columns={
            "In force": "year",
            "§ 32a Abs. 5 & 6 (joint)": "joint",
            "Otherwise (single)": "single",
        }).apply(pd.to_numeric, errors="coerce")
        return df.set_index("year")

    def _soli(self, itax: float, hh_type: int | None, year: int) -> float:
        """Return solidarity-surcharge amount for a given income-tax value.

        Fallback rules   
        1. Use the row for *the latest year ≤ survey* that exists.  
        2. If the table is empty (or only contains years > survey), apply
           the flat 5.5 % rate.
        """
        joint = hh_type in {2, 4, 5, 6}
        col   = "joint" if joint else "single"

        # 1) exact hit
        if year in self.soli_table.index:
            thresh = self.soli_table.at[year, col]
        else:
            # 2) fallback to latest year before the survey year
            older = self.soli_table[self.soli_table.index < year]
            if older.empty:                      # nothing at all → flat rate
                return math.floor(itax * 0.055)
            thresh = older.iloc[-1][col]

        return 0 if itax <= thresh else math.floor(itax * 0.055)

    def _income_tax(self, income: float | None, year: int) -> float | None:
        if pd.isna(income): 
            return None
        p = self.tax_table[self.tax_table.year == year].iloc[0]
        A = p["basic_allowance (1) 1"]
        B = p["first_bracket_upper (1) 2"]
        C = p["second_bracket_upper (1) 3"]
        D = p["top_rate_threshold (1) 4"]
        income = math.floor(income)
        if income <= A: 
            tax = 0
        elif income <= B:
            y = (income - A) / 1e4
            tax = (p["bracket2_a (1) 2"] * y + p["bracket2_b (1) 2"]) * y
        elif income <= C:               
            z = (income - B) / 1e4
            tax = (p["bracket3_a (1) 3"] * z + p["bracket3_b (1) 3"]) * z + p["bracket3_c (1) 3"]
        elif income <= D:                               
            tax = p["Rate_4 (1) 4"] * income - p["Deduct_4 (1) 4"]
        else:                                           
            tax = p["Rate_5 (1) 5"] * income - p["Deduct_5 (1) 5"]
        return math.floor(tax)
