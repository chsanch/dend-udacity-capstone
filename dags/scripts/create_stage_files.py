import pandas as pd
import os


def parse_json(json_file):
    catalog = pd.read_json(json_file, orient="columns")
    pubyear = catalog["pubyear"].str.extract(r"(\d{4})", expand=False)
    catalog["clean_pubyear"] = pubyear
    author = (
        catalog["author"]
        .str.extract(r"([\w]+-?[^\d\(\)\-]+)", expand=False)
        .str.strip()
    )
    catalog["clean_author"] = author
    title = (
        catalog["title"]
        .str.strip()
        .str.rstrip("\s\t/")
        .str.rstrip()
        .str.replace("\s:", ":", regex=True)
        .str.replace("\s{2,}", " ", regex=True)
        .str.replace("*", "", regex=True)
    )
    catalog["clean_title"] = title
    publisher = catalog["publisher"].str.strip().str.rstrip(",:;/").str.strip()
    catalog["clean_publisher"] = publisher
    isbn = catalog["isbn"].str.strip()
    catalog["clean_isbn"] = isbn
    catalog["book_id"] = [i + 1 for i in range(len(catalog.index))]

    return catalog


def get_books(catalog):

    books = catalog.loc[
        :,
        [
            "book_id",
            "clean_title",
            "clean_author",
            "clean_publisher",
            "clean_pubyear",
            "clean_isbn",
        ],
    ]

    books.columns = ["book_id", "title", "author", "publisher", "pubyear", "isbn"]

    return books


def get_items(catalog):
    locations = catalog.loc[:, ["book_id", "locations"]]
    items = []

    for row in locations.itertuples():
        for r in row[2].keys():
            item_location = r.split(".")
            district = item_location[0].strip() if len(item_location) > 1 else None
            library = (
                item_location[1].strip()
                if len(item_location) > 1
                else item_location[0].strip()
            )

            for i in row[2][r]:
                i = i.strip()
                items.append(
                    {
                        "book_id": row[1],
                        "district": district,
                        "library": library,
                        "prbarc": i,
                    }
                )

    items_df = pd.DataFrame(items)

    return items_df


def df_to_csv(df, output_file):
    df.to_csv(output_file, header=False, index=False, sep="|")


def main(json_file, books_file, items_file):

    if os.path.isfile(books_file) and os.path.isfile(items_file):
        print("Stage files already created.")
    else:
        print("Creating catalog file")
        catalog = parse_json(json_file)
        print("Creating books json file")
        books = get_books(catalog)
        df_to_csv(books, books_file)
        print("Creating items json file")
        items = get_items(catalog)
        df_to_csv(items, items_file)


if __name__ == "__main__":
    main()
