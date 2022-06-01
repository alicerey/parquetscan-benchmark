SELECT MIN(kt.kind) AS movie_kind, MIN(t.title) AS complete_us_internet_movie FROM external('complete_cast.parquet') cc, external('comp_cast_type.parquet') cct1, external('company_name.parquet') cn, external('company_type.parquet') ct, external('info_type.parquet') it1, external('keyword.parquet') k, external('kind_type.parquet') kt, external('movie_companies.parquet') mc, external('movie_info.parquet') mi, external('movie_keyword.parquet') mk, external('title.parquet') t WHERE cct1.kind  = 'complete+verified' AND cn.country_code  = '[us]' AND it1.info  = 'release dates' AND kt.kind  in ('movie', 'tv movie', 'video movie', 'video game') AND mi.note  like '%internet%' AND mi.info  is not NULL and (mi.info like 'USA:% 199%' or mi.info like 'USA:% 200%') AND t.production_year  > 1990 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mc.movie_id AND t.id = cc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mc.movie_id AND mk.movie_id = cc.movie_id AND mi.movie_id = mc.movie_id AND mi.movie_id = cc.movie_id AND mc.movie_id = cc.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND cn.id = mc.company_id AND ct.id = mc.company_type_id AND cct1.id = cc.status_id;