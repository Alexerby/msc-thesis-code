from descriptives.helpers import load_data
from descriptives.plot_utils import plot_comparison, plot_variable, plot_multiple_timelines

def main(): 

    main_df = load_data("bafoeg_calculations", from_parquet=True)

    plot_variable(
            main_df,
            var = "theoretical_bafög",
            plot_type = "timeline",
            timevar = "syear",
            drop_zeros = True,
            title = "Mean of Theoretical BAföG",
            save_path="~/Documents/MScEcon/Semester 2/Master Thesis I/thesis/figures/timeline/theo.png"
    )

    plot_variable(
            main_df,
            var = "reported_bafög",
            plot_type = "timeline",
            timevar = "syear",
            drop_zeros = True,
            title = "Mean of Reported BAföG",
            save_path="~/Documents/MScEcon/Semester 2/Master Thesis I/thesis/figures/timeline/reported.png"
    )

    plot_multiple_timelines(
        main_df,
        vars=["theoretical_bafög", "reported_bafög"],
        timevar="syear",
        drop_zeros=True,
        title="Mean of Theoretical and Reported BAföG Over Time",
        save_path="~/Documents/MScEcon/Semester 2/Master Thesis I/thesis/figures/timeline/comparison.png"
    )

if __name__ == "__main__":
    main()
