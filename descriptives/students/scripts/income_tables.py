from .helper import load_student_data, get_output_paths

from ..tables.income import summarize_gross_income, summarize_excess_income



def run_income_summary(save: bool = True, print_preview: bool = False):
    """
    Generate and optionally save gross and excess income summary tables.
    """
    df = load_student_data()
    paths = get_output_paths()

    gross_summary = summarize_gross_income(df, print_table=True)
    excess_summary = summarize_excess_income(df, print_table=True)

    if print_preview:
        print(gross_summary.head())
        print(excess_summary.head())

    if save:
        gross_summary.to_csv(paths["gross_income"], index=False)
        excess_summary.to_csv(paths["excess_income"], index=False)
        print(f"Saved gross income summary to: {paths['gross_income']}")
        print(f"Saved excess income summary to: {paths['excess_income']}")


def main():
    run_income_summary()


if __name__ == "__main__":
    main()
