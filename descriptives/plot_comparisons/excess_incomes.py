import numpy as np
from descriptives.helpers import load_data
from descriptives.plot_utils import plot_variable

def main(): 
    main_df = load_data("bafoeg_calculations", from_parquet=True)

    # Unlog the excess income variables by exponentiating
    main_df['excess_income_par'] = np.exp(main_df['excess_income_par'])
    main_df['excess_income_stu'] = np.exp(main_df['excess_income_stu'])

    plot_variable(
        main_df,
        "excess_income_par",
        "pdf",
        title="Distribution of Student Excess Income",
        drop_small = 5,
        trim_percentile = 5,
        save_path="~/Documents/MScEcon/Semester 2/Master Thesis I/thesis/figures/distributions/par_excess_income.png"
    )

    plot_variable(
        main_df,
        "excess_income_stu",
        "pdf",
        title="Distribution of Student Excess Income",
        drop_small = 5,
        trim_percentile = 5,
        save_path="~/Documents/MScEcon/Semester 2/Master Thesis I/thesis/figures/distributions/stu_excess_income.png"
    )

if __name__ == "__main__":
    main()
