import wget

url = "https://datos.madrid.es/egob/catalogo/200081-1-catalogo-bibliotecas.gz"

wget.download(url, "../datasets/catalog.gz")
