SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS western_violent_movie FROM umbra.parquetview('company_name.parquet') AS cn, umbra.parquetview('company_type.parquet') AS ct, umbra.parquetview('info_type.parquet') AS it1, umbra.parquetview('info_type.parquet') AS it2, umbra.parquetview('keyword.parquet') AS k, umbra.parquetview('kind_type.parquet') AS kt, umbra.parquetview('movie_companies.parquet') AS mc, umbra.parquetview('movie_info.parquet') AS mi, umbra.parquetview('movie_info_idx.parquet') AS mi_idx, umbra.parquetview('movie_keyword.parquet') AS mk, umbra.parquetview('title.parquet') AS t WHERE cn.country_code  != '[us]' AND it1.info  = 'countries' AND it2.info  = 'rating' AND k.keyword  in ('murder', 'murder-in-title', 'blood', 'violence') AND kt.kind  in ('movie', 'episode') AND mc.note  not like '%(USA)%' and mc.note like '%(200%)%' AND mi.info IN ('Germany', 'German', 'USA', 'American') AND mi_idx.info  < '7.0' AND t.production_year  > 2008 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mi_idx.movie_id AND t.id = mc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mi_idx.movie_id AND mk.movie_id = mc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mc.movie_id = mi_idx.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id AND ct.id = mc.company_type_id AND cn.id = mc.company_id;