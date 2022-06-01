SELECT MIN(t.title) AS movie_title FROM external('keyword.parquet') k, external('movie_info.parquet') mi, external('movie_keyword.parquet') mk, external('title.parquet') t WHERE k.keyword  like '%sequel%' AND mi.info  IN ('Bulgaria') AND t.production_year > 2010 AND t.id = mi.movie_id AND t.id = mk.movie_id AND mk.movie_id = mi.movie_id AND k.id = mk.keyword_id;
