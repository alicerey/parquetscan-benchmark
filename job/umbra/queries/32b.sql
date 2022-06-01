SELECT MIN(lt.link) AS link_type, MIN(t1.title) AS first_movie, MIN(t2.title) AS second_movie FROM umbra.parquetview('keyword.parquet') AS k, umbra.parquetview('link_type.parquet') AS lt, umbra.parquetview('movie_keyword.parquet') AS mk, umbra.parquetview('movie_link.parquet') AS ml, umbra.parquetview('title.parquet') AS t1, umbra.parquetview('title.parquet') AS t2 WHERE k.keyword ='character-name-in-title' AND mk.keyword_id = k.id AND t1.id = mk.movie_id AND ml.movie_id = t1.id AND ml.linked_movie_id = t2.id AND lt.id = ml.link_type_id AND mk.movie_id = t1.id;
