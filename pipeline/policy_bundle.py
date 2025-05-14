import pandas as pd
from dataclasses import dataclass
from data_handler import SOEPStatutoryInputs

@dataclass
class PolicyTableBundle:
    werbung: pd.DataFrame
    needs: pd.DataFrame
    insurance: pd.DataFrame
    social_insurance: pd.DataFrame
    individual_allowance: pd.DataFrame
    allowance_25: pd.DataFrame

    @classmethod
    def from_statutory_inputs(cls) -> "PolicyTableBundle":
        def load(name, columns=lambda _: True):
            table = SOEPStatutoryInputs(name)
            table.load_dataset(columns=columns)
            return table.data.copy()

        return cls(
            werbung=load("Werbungskostenpauschale", ["Year", "werbungskostenpauschale"]),
            needs=load("Basic Allowances - § 13"),
            insurance=load("Basic Allowances - § 13a"),
            allowance_25=load("Basic Allowances - § 25"),
            individual_allowance=load("Basic Allowances - § 23"),
            social_insurance=load("Sozialversicherung - § 21"),
        )
