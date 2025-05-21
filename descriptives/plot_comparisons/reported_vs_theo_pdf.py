from descriptives.helpers import load_data
from descriptives.plot_utils import plot_comparison, plot_variable

def main(): 

    main_df = load_data("bafoeg_calculations", from_parquet=True)

    plot_variable(
            main_df,
            var = "theoretical_bafög",
            plot_type = "pdf",
            drop_zeros = True,
            title = "Distribution of Theoretical BAföG",
            save_path="~/Documents/MScEcon/Semester 2/Master Thesis I/thesis/figures/distributions/theo.png"
    )

    plot_variable(
            main_df,
            var = "reported_bafög",
            plot_type = "pdf",
            drop_zeros = True,
            title = "Distribution of Reported BAföG",
            save_path="~/Documents/MScEcon/Semester 2/Master Thesis I/thesis/figures/distributions/reported.png"
    )

if __name__ == "__main__":
    main()
