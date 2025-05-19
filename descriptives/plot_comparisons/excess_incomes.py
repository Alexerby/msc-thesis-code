from descriptives.helpers import load_data
from descriptives.plot_utils import plot_comparison, plot_variable

def main(): 

    main_df = load_data("bafoeg_calculations", from_parquet=True)

    plot_variable(
            main_df,
            "excess_income_par",
            "pdf",
    )

    plot_variable(
            main_df,
            "excess_income_stu",
            "pdf",
    )

if __name__ == "__main__":
    main()
