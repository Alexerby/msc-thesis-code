
import pandas as pd
from pathlib import Path
from descriptives.core.plot_utils import plot_variable, plot_comparison, plot_pdf_per_year
from descriptives.helpers import get_output_paths, load_data

df = load_data("bafoeg_calculations", from_parquet=True)

# Set output directory
output_dir = get_output_paths("pdf_dir")

# Plot PDFs per year for a variable (e.g., "monthly_award")
# plot_pdf_per_year(df, var="theoretical_bafög", output_dir=output_dir)


output_path = Path("~/Downloads/plots/excess_stu_pdf_comparison.png").expanduser()
plot_comparison(
    df=df,
    var1="reported_bafög",
    var2="theoretical_bafög",
    plot_type="timeline",
    timevar="syear",
    drop_zeros=True,
    title1="Reported BAföG",
    title2="Theoretical BAföG",
    save_path=output_path
)




# output_base = Path("~/Downloads/plots/bafoeg_pdf_comparison_per_year").expanduser()
# output_base.mkdir(parents=True, exist_ok=True)
#
# for year in sorted(df["syear"].dropna().unique()):
#     sub_df = df[df["syear"] == year]
#
#     if sub_df[["reported_bafög", "theoretical_bafög"]].dropna().eq(0).all().all():
#         continue  # Skip if both columns are all zero or NaN
#
#     output_path = output_base / f"bafoeg_timeline_comparison_{year}.png"
#
#     plot_comparison(
#         df=sub_df,
#         var1="reported_bafög",
#         var2="theoretical_bafög",
#         plot_type="pdf",
#         drop_zeros=True,
#         title1=f"Reported BAföG ({year})",
#         title2=f"Theoretical BAföG ({year})",
#         save_path=output_path
#     )
