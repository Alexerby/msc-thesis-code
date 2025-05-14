from descriptives.helpers import get_output_paths, load_data
from descriptives.bafoeg_calculations.plots import *
# plot_variable, plot_comparison


def run_income_plots():
    """
    Execute all income-related plots and save them to the appropriate output directories.
    Loads data from cached Parquet files in ~/Downloads/bafoeg_results.
    """
    # Prepare output paths
    paths = get_output_paths()

    # Load cached data from Parquet
    students_df = load_data("students", source="parquet")
    parents_df = load_data("parents", source="parquet")
    siblings_df = load_data("siblings", source="parquet")
    bafoeg_calc = load_data("bafoeg_calculations", source="parquet")


    plot_pdf_per_year(bafoeg_calc, "theoretical_bafög", paths["pdf_dir"])
    plot_pdf_per_year(bafoeg_calc, "plc0168_h", paths["pdf_dir"])
    # # === Side-by-side comparison: theoretical vs. reported
    # plot_comparison(
    #     bafoeg_calc,
    #     var1="theoretical_bafög",
    #     var2="plc0168_h",
    #     plot_type="pdf",
    #     title1="Theoretical BAföG",
    #     title2="Reported BAföG",
    #     save_path=paths["pdf_dir"] / "bafoeg_theory_vs_reported_pdf.png"
    # )
    #
    #
    # plot_comparison(
    #     bafoeg_calc,
    #     var1="theoretical_bafög",
    #     var2="plc0168_h",
    #     plot_type="timeline",
    #     timevar="syear",
    #     title1="Theoretical BAföG",
    #     title2="Reported BAföG",
    #     save_path=paths["income_figures_dir"] / "bafoeg_theory_vs_reported_timeline.png"
    # )
    #
    #
    # plot_comparison(
    #     bafoeg_calc,
    #     var1="theoretical_bafög",
    #     var2="plc0168_h",
    #     plot_type="pdf",
    #     title1="Theoretical BAföG",
    #     title2="Reported BAföG",
    #     save_path=paths["pdf_dir"] / "bafoeg_theory_vs_reported_pdf.png"
    # )
    #
    # # === Individual plots for theoretical_bafög
    # plot_variable(
    #     bafoeg_calc,
    #     var="theoretical_bafög",
    #     plot_type="timeline",
    #     timevar="syear",
    #     save_path=paths["income_figures_dir"] / "theoretical_bafög_timeline.png"
    # )
    #
    # plot_variable(
    #     bafoeg_calc,
    #     var="theoretical_bafög",
    #     plot_type="scatter",
    #     timevar="syear",
    #     save_path=paths["income_figures_dir"] / "theoretical_bafög_scatter.png"
    # )
    #
    # plot_variable(
    #     bafoeg_calc,
    #     var="theoretical_bafög",
    #     plot_type="pdf",
    #     drop_zeros=True,
    #     save_path=paths["pdf_dir"] / "theoretical_bafög_pdf.png"
    # )


if __name__ == "__main__": 
    run_income_plots()
