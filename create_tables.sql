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
    prbarc text
);

CREATE TABLE IF NOT EXISTS public.loans (
    prbarc text,
    prcolp text,
    prfpre date
);

CREATE TABLE IF NOT EXISTS public.book_loans (
    book_id bigint PRIMARY KEY,
    title text,
    publiser text,
    author text,
    item_code text,
    library text,
    district text,
    user_type text,
    loan_date date
);

GRANT pg_read_server_files TO biblio;

