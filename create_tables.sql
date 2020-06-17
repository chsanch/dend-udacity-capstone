DROP TABLE IF EXISTS public.books;

DROP TABLE IF EXISTS public.books_locations;

DROP TABLE IF EXISTS public.loans;

DROP TABLE IF EXISTS public.book_loans;

CREATE TABLE IF NOT EXISTS public.books (
    book_id bigint PRIMARY KEY,
    title text,
    author text,
    publisher text,
    pubyear int,
    isbn text
);

CREATE TABLE IF NOT EXISTS public.books_locations (
    book_id bigint,
    district text,
    library text,
    prbarc bigint
);

CREATE TABLE IF NOT EXISTS public.loans (
    prbarc bigint,
    prcolp text,
    prfpre date
);

CREATE TABLE IF NOT EXISTS public.book_loans (
    book_id bigint,
    title text,
    publisher text,
    author text,
    item_code bigint,
    library text,
    district text,
    user_type text,
    loan_date date
);

CREATE INDEX ON books_locations (prbarc);

CREATE INDEX ON loans (prbarc);

CREATE INDEX ON book_loans (book_id);

CREATE INDEX ON book_loans (item_code);

GRANT pg_read_server_files TO biblio;

