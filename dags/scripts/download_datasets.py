import wget
import gzip
import shutil
import os


def download_dataset(file_name, url):

    if os.path.isfile(file_name):
        print(f"** {file_name} already exists")
    else:
        print(f"** downloading {url}")
        wget.download(url, file_name)


def download_catalog(path):
    url = "https://datos.madrid.es/egob/catalogo/200081-1-catalogo-bibliotecas.gz"
    file_path = f"{path}/catalog.gz"
    download_dataset(file_path, url)

    marc_file = f"{path}/catalog_marc"

    if os.path.isfile(marc_file):
        print("** Marc file already exists")
    else:
        catalog_file = f"{path}/catalog.gz"
        print("** Creating Marc file")
        with gzip.open(catalog_file, "r") as f_in, open(marc_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def main(datasets_folder):
    print("Get catalog: ")
    download_catalog(datasets_folder)

    print("Get loan datasets: ")
    dataset_list = {
        "february-2018.csv": "https://datos.madrid.es/egob/catalogo/212700-80-bibliotecas-prestamos-historico.csv",
        "march-2018.csv": "https://datos.madrid.es/egob/catalogo/212700-82-bibliotecas-prestamos-historico.csv",
        "april-2018.csv": "https://datos.madrid.es/egob/catalogo/212700-84-bibliotecas-prestamos-historico.csv",
    }

    for name, url in dataset_list.items():
        file_name = f"{datasets_folder}/{name}"
        download_dataset(file_name, url)


if __name__ == "__main__":
    main()
