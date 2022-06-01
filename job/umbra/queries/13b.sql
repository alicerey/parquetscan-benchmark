SELECT MIN(cn.name) AS producing_company, MIN(miidx.info) AS rating, MIN(t.title) AS movie_about_winning FROM umbra.parquetview('company_name.parquet') AS cn, umbra.parquetview('company_type.parquet') AS ct, umbra.parquetview('info_type.parquet') AS it, umbra.parquetview('info_type.parquet') AS it2, umbra.parquetview('kind_type.parquet') AS kt, umbra.parquetview('movie_companies.parquet') AS mc, umbra.parquetview('movie_info.parquet') AS mi, umbra.parquetview('movie_info_idx.parquet') AS miidx, umbra.parquetview('title.parquet') AS t WHERE cn.country_code ='[us]' AND ct.kind ='production companies' AND it.info ='rating' AND it2.info ='release dates' AND kt.kind ='movie' AND t.title  != '' AND (t.title LIKE '%Champion%' OR t.title LIKE '%Loser%') AND mi.movie_id = t.id AND it2.id = mi.info_type_id AND kt.id = t.kind_id AND mc.movie_id = t.id AND cn.id = mc.company_id AND ct.id = mc.company_type_id AND miidx.movie_id = t.id AND it.id = miidx.info_type_id AND mi.movie_id = miidx.movie_id AND mi.movie_id = mc.movie_id AND miidx.movie_id = mc.movie_id;
