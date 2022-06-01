SELECT MIN(cn.name) AS from_company, MIN(mc.note) AS production_note, MIN(t.title) AS movie_based_on_book FROM umbra.parquetview('company_name.parquet') AS cn, umbra.parquetview('company_type.parquet') AS ct, umbra.parquetview('keyword.parquet') AS k, umbra.parquetview('link_type.parquet') AS lt, umbra.parquetview('movie_companies.parquet') AS mc, umbra.parquetview('movie_keyword.parquet') AS mk, umbra.parquetview('movie_link.parquet') AS ml, umbra.parquetview('title.parquet') AS t WHERE cn.country_code  !='[pl]' AND ct.kind  != 'production companies' and ct.kind is not NULL AND k.keyword  in ('sequel', 'revenge', 'based-on-novel') AND mc.note  is not NULL AND t.production_year  > 1950 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id;