import pandas as pd


from .helpers import get_output_paths, load_data


# from ..plots.income_plots import (
#     plot_gross_income_over_time,
#     plot_gross_income_pdfs_over_time,
#     plot_excess_income_over_time,
#     plot_excess_income_pdfs_over_time,
# )

from descriptives.students.plots.plots import plot_variable
# from ..plots.income_plots import plot_variable




def run_income_plots():
    """
    Execute all income-related plots and save them to disk.
    """
    paths = get_output_paths()
    students_df = load_data("students")
    parents_df = load_data("parents")
    siblings_df = load_data("siblings")

    # Plot income timeline, excluding 0 and NaN values
    # plot_variable(siblings_df, var="excess_income", plot_type="timeline", timevar="syear")
    # plot_variable(siblings_df, var="excess_income", plot_type="scatter", timevar="syear")
    # plot_variable(siblings_df, var="excess_income", plot_type="pdf", drop_zeros=True)


    plot_variable(siblings_df, var="net_monthly_income", plot_type="timeline", timevar="syear")
    plot_variable(siblings_df, var="net_monthly_income", plot_type="scatter", timevar="syear")
    plot_variable(siblings_df, var="net_monthly_income", plot_type="pdf", drop_zeros=True)

def main():
    run_income_plots()


if __name__ == "__main__":
    main()
