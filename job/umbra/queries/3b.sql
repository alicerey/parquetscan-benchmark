SELECT MIN(t.title) AS movie_title FROM umbra.parquetview('keyword.parquet') AS k, umbra.parquetview('movie_info.parquet') AS mi, umbra.parquetview('movie_keyword.parquet') AS mk, umbra.parquetview('title.parquet') AS t WHERE k.keyword  like '%sequel%' AND mi.info  IN ('Bulgaria') AND t.production_year > 2010 AND t.id = mi.movie_id AND t.id = mk.movie_id AND mk.movie_id = mi.movie_id AND k.id = mk.keyword_id;