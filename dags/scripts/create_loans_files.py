import pandas as pd
import os


def get_loans(csv_file):
    print(f"Processing {csv_file}")
    loans_data = pd.read_csv(
        csv_file,
        sep=";",
        error_bad_lines=False,
        encoding="iso8859-1",
        parse_dates=["prfpre"],
        quotechar='"',
    )
    prcocs = loans_data["prcocs"].str.strip()
    loans_data["clean_prcocs"] = prcocs
    prcolp = loans_data["prcolp"].str.strip()
    loans_data["clean_prcolp"] = prcolp
    loans = loans_data.loc[
        loans_data["clean_prcocs"] == "LIB",
        ["prbarc", "clean_prcolp", "prfpre"],
    ]
    loans.columns = ["prbarc", "prcolp", "prfpre"]

    return loans


def df_to_csv(df, output_file):
    print(f"Creating {output_file}")
    df.to_csv(output_file, header=False, index=False)


def main(datasets_folder):
    list_loan_data = ["february-2018", "march-2018", "april-2018"]

    for item in list_loan_data:
        input_file = f"{datasets_folder}/{item}.csv"
        output_file = f"{datasets_folder}/{item}_stage.csv"

        if os.path.isfile(input_file):
            if os.path.isfile(output_file):
                print(f"{output_file} already exists")
            else:
                df = get_loans(input_file)
                df_to_csv(df, output_file)
        else:
            print(f"You have to download {input_file} first")


if __name__ == "__main__":
    main()
