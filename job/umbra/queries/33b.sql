SELECT MIN(cn1.name) AS first_company, MIN(cn2.name) AS second_company, MIN(mi_idx1.info) AS first_rating, MIN(mi_idx2.info) AS second_rating, MIN(t1.title) AS first_movie, MIN(t2.title) AS second_movie FROM umbra.parquetview('company_name.parquet') AS cn1, umbra.parquetview('company_name.parquet') AS cn2, umbra.parquetview('info_type.parquet') AS it1, umbra.parquetview('info_type.parquet') AS it2, umbra.parquetview('kind_type.parquet') AS kt1, umbra.parquetview('kind_type.parquet') AS kt2, umbra.parquetview('link_type.parquet') AS lt, umbra.parquetview('movie_companies.parquet') AS mc1, umbra.parquetview('movie_companies.parquet') AS mc2, umbra.parquetview('movie_info_idx.parquet') AS mi_idx1, umbra.parquetview('movie_info_idx.parquet') AS mi_idx2, umbra.parquetview('movie_link.parquet') AS ml, umbra.parquetview('title.parquet') AS t1, umbra.parquetview('title.parquet') AS t2 WHERE cn1.country_code  = '[nl]' AND it1.info  = 'rating' AND it2.info  = 'rating' AND kt1.kind  in ('tv series') AND kt2.kind  in ('tv series') AND lt.link  LIKE '%follow%' AND mi_idx2.info  < '3.0' AND t2.production_year  = 2007 AND lt.id = ml.link_type_id AND t1.id = ml.movie_id AND t2.id = ml.linked_movie_id AND it1.id = mi_idx1.info_type_id AND t1.id = mi_idx1.movie_id AND kt1.id = t1.kind_id AND cn1.id = mc1.company_id AND t1.id = mc1.movie_id AND ml.movie_id = mi_idx1.movie_id AND ml.movie_id = mc1.movie_id AND mi_idx1.movie_id = mc1.movie_id AND it2.id = mi_idx2.info_type_id AND t2.id = mi_idx2.movie_id AND kt2.id = t2.kind_id AND cn2.id = mc2.company_id AND t2.id = mc2.movie_id AND ml.linked_movie_id = mi_idx2.movie_id AND ml.linked_movie_id = mc2.movie_id AND mi_idx2.movie_id = mc2.movie_id;
