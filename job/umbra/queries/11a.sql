SELECT MIN(cn.name) AS from_company, MIN(lt.link) AS movie_link_type, MIN(t.title) AS non_polish_sequel_movie FROM umbra.parquetview('company_name.parquet') AS cn, umbra.parquetview('company_type.parquet') AS ct, umbra.parquetview('keyword.parquet') AS k, umbra.parquetview('link_type.parquet') AS lt, umbra.parquetview('movie_companies.parquet') AS mc, umbra.parquetview('movie_keyword.parquet') AS mk, umbra.parquetview('movie_link.parquet') AS ml, umbra.parquetview('title.parquet') AS t WHERE cn.country_code !='[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') AND ct.kind ='production companies' AND k.keyword ='sequel' AND lt.link LIKE '%follow%' AND mc.note IS NULL AND t.production_year BETWEEN 1950 AND 2000 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id;
