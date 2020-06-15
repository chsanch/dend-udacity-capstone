CREATE TABLE public.books_staging (
    book_id bigint PRIMARY KEY,
    title text,
    author text,
    publisher text,
    pubyear int,
    isbn text
);

CREATE TABLE public.books_locations (
    book_id bigint,
    district text,
    library text,
    prbarc text
);

CREATE TABLE public.loans_staging (
    prbarc text,
    prcolp text,
    prfpre date
);

