SELECT MIN(kt.kind) AS movie_kind, MIN(t.title) AS complete_us_internet_movie FROM 'complete_cast.parquet' cc, 'comp_cast_type.parquet' cct1, 'company_name.parquet' cn, 'company_type.parquet' ct, 'info_type.parquet' it1, 'keyword.parquet' k, 'kind_type.parquet' kt, 'movie_companies.parquet' mc, 'movie_info.parquet' mi, 'movie_keyword.parquet' mk, 'title.parquet' t WHERE cct1.kind  = 'complete+verified' AND cn.country_code  = '[us]' AND it1.info  = 'release dates' AND kt.kind  in ('movie') AND mi.note  like '%internet%' AND mi.info  is not NULL and (mi.info like 'USA:% 199%' or mi.info like 'USA:% 200%') AND t.production_year  > 2000 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mc.movie_id AND t.id = cc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mc.movie_id AND mk.movie_id = cc.movie_id AND mi.movie_id = mc.movie_id AND mi.movie_id = cc.movie_id AND mc.movie_id = cc.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND cn.id = mc.company_id AND ct.id = mc.company_type_id AND cct1.id = cc.status_id;
