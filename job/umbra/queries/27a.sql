SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel FROM umbra.parquetview('complete_cast.parquet') AS cc, umbra.parquetview('comp_cast_type.parquet') AS cct1, umbra.parquetview('comp_cast_type.parquet') AS cct2, umbra.parquetview('company_name.parquet') AS cn, umbra.parquetview('company_type.parquet') AS ct, umbra.parquetview('keyword.parquet') AS k, umbra.parquetview('link_type.parquet') AS lt, umbra.parquetview('movie_companies.parquet') AS mc, umbra.parquetview('movie_info.parquet') AS mi, umbra.parquetview('movie_keyword.parquet') AS mk, umbra.parquetview('movie_link.parquet') AS ml, umbra.parquetview('title.parquet') AS t WHERE cct1.kind  in ('cast', 'crew') AND cct2.kind  = 'complete' AND cn.country_code !='[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') AND ct.kind ='production companies' AND k.keyword ='sequel' AND lt.link LIKE '%follow%' AND mc.note IS NULL AND mi.info IN ('Sweden', 'Germany','Swedish', 'German') AND t.production_year BETWEEN 1950 AND 2000 AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND mi.movie_id = t.id AND t.id = cc.movie_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id AND ml.movie_id = mk.movie_id AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id AND ml.movie_id = mi.movie_id AND mk.movie_id = mi.movie_id AND mc.movie_id = mi.movie_id AND ml.movie_id = cc.movie_id AND mk.movie_id = cc.movie_id AND mc.movie_id = cc.movie_id AND mi.movie_id = cc.movie_id;