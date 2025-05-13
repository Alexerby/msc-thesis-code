import pandas as pd
from dataclasses import dataclass
from data_handler import SOEPStatutoryInputs

@dataclass
class PolicyTableBundle:
    werbung: pd.DataFrame
    allowance: pd.DataFrame
    needs: pd.DataFrame
    student_allowance: pd.DataFrame
    insurance: pd.DataFrame
    social_insurance: pd.DataFrame

    @classmethod
    def from_statutory_inputs(cls) -> "PolicyTableBundle":
        def load(name, columns=lambda _: True):
            table = SOEPStatutoryInputs(name)
            table.load_dataset(columns=columns)
            return table.data.copy()

        return cls(
            werbung=load("Werbungskostenpauschale", ["Year", "werbungskostenpauschale"]),
            allowance=load("Basic Allowances - § 25"),
            needs=load("Basic Allowances - § 13"),
            student_allowance=load("Basic Allowances - § 23"),
            insurance=load("Basic Allowances - § 13a"),
            social_insurance=load("Sozialversicherung - § 21"),
        )
