SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS mainstream_movie FROM umbra.parquetview('company_name.parquet') AS cn, umbra.parquetview('company_type.parquet') AS ct, umbra.parquetview('info_type.parquet') AS it1, umbra.parquetview('info_type.parquet') AS it2, umbra.parquetview('movie_companies.parquet') AS mc, umbra.parquetview('movie_info.parquet') AS mi, umbra.parquetview('movie_info_idx.parquet') AS mi_idx, umbra.parquetview('title.parquet') AS t WHERE cn.country_code  = '[us]' AND ct.kind  = 'production companies' AND it1.info = 'genres' AND it2.info = 'rating' AND mi.info  in ('Drama', 'Horror', 'Western', 'Family') AND mi_idx.info  > '7.0' AND t.production_year  between 2000 and 2010 AND t.id = mi.movie_id AND t.id = mi_idx.movie_id AND mi.info_type_id = it1.id AND mi_idx.info_type_id = it2.id AND t.id = mc.movie_id AND ct.id = mc.company_type_id AND cn.id = mc.company_id AND mc.movie_id = mi.movie_id AND mc.movie_id = mi_idx.movie_id AND mi.movie_id = mi_idx.movie_id;
