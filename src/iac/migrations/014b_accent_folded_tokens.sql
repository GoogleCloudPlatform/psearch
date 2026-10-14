ALTER TABLE products ADD COLUMN title_tokens_folded TOKENLIST AS (TOKENIZE_FULLTEXT(REGEXP_REPLACE(NORMALIZE(title, NFD), r'\p{M}', ''))) HIDDEN;
ALTER TABLE products ADD COLUMN description_tokens_folded TOKENLIST AS (TOKENIZE_FULLTEXT(REGEXP_REPLACE(NORMALIZE(description, NFD), r'\p{M}', ''))) HIDDEN;
ALTER SEARCH INDEX products_text_idx ADD COLUMN title_tokens_folded;
ALTER SEARCH INDEX products_text_idx ADD COLUMN description_tokens_folded
