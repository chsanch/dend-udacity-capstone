class SqlQueries:
    book_loans_insert = """
        SELECT
            b.book_id,
            b.title,
            b.publisher,
            b.author,
            l.prbarc AS item_code,
            b.isbn,
            bl.library,
            bl.district,
            l.prcolp AS user_type,
            l.prfpre AS loan_date
        FROM
            loans l,
            books_locations bl,
            books b
        WHERE
            l.prbarc = bl.prbarc
            AND b.book_id = bl.book_id    
    """
