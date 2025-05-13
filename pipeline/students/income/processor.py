import pandas as pd 
import numpy as np

from pipeline.soep_bundle import SOEPDataBundle
from pipeline.policy_bundle import PolicyTableBundle

from pipeline.common.processor_income import merge_income
from pipeline.common.processor_deductions import (
        apply_table_deduction, 
        apply_tax, 
        apply_entity_allowances
)

from services.tax import TaxService

def add_income(df: pd.DataFrame, data: SOEPDataBundle, policy: PolicyTableBundle) -> pd.DataFrame:
    return (
        df.copy()
        # 1. Merge gross income from pkal
        .pipe(merge_income, data.pkal)

        # 2. Apply Werbungskostenpauschale (fixed amounts by year)
        .pipe(
            apply_table_deduction,
            table=policy.werbung,
            merge_left_on="syear",
            merge_right_on="Year",
            deduction_column="werbungskostenpauschale",
            input_col="gross_annual_income",
            output_col="inc_w",
            method="max_zero",
            deduction_type="amount",
            drop_deduction_column=True
        )

        # 3. Apply Sozialversicherungs-Pauschale (year-varying rates, forward filled, with cap)
        .pipe(
            apply_table_deduction,
            table=policy.social_insurance,
            merge_left_on="syear",
            merge_right_on="Year",
            deduction_column="Rate",
            input_col="inc_w",
            output_col="inc_si",
            method="default",
            deduction_type="rate",
            forward_fill=True,
            cap=17_200,
            drop_deduction_column=True
        )

        # 4. Apply taxes 
        .pipe(
            apply_tax,
            tax_service=TaxService("inc_si"), 
            input_col="inc_si", 
            output_col="inc_net"
        )

        # 4.5. Compute monthly net income for deduction logic
        .assign(net_monthly_income = lambda d: d["inc_net"] / 12)

        # 5. Apply personal + family-based allowance deductions to compute excess income
        .pipe(
            apply_entity_allowances,
            allowance_table=policy.student_allowance.rename(columns={
                "§ 23 (1) 1": "allowance_self",
                "§ 23 (1) 2": "allowance_spouse",
                "§ 23 (1) 3": "allowance_child"
            }),
            entity_keys={
                "self": "allowance_self",
                "spouse": "allowance_spouse",
                "child": "allowance_child"
            },
            entity_counts={
                "spouse": "has_partner",
                "child": "num_children"
            },
            income_col="net_monthly_income",
            output_col="excess_income"
        )
    )
