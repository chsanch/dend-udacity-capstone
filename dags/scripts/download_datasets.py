import wget
import gzip
import shutil
import os


def download_dataset(file_name, url):

    if os.path.isfile(file_name):
        print(f"** {file_name} already exists")
    else:
        print(f"** downloading {url}")
        wget.download(url, file_name, bar=None)


def download_catalog(path, marc_file, url):
    catalog_file = f"{path}/catalog.gz"
    download_dataset(catalog_file, url)

    if os.path.isfile(marc_file):
        print("** Marc file already exists")
    else:
        print("** Creating Marc file")
        with gzip.open(catalog_file, "r") as f_in, open(marc_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def main(datasets_folder, datasets_list, marc_file, catalog_url):
    print("Get catalog: ")
    download_catalog(datasets_folder, marc_file, catalog_url)

    print("Get loan datasets: ")

    for name, url in datasets_list.items():
        file_name = f"{datasets_folder}/{name}"
        download_dataset(file_name, url)


if __name__ == "__main__":
    main()
