SELECT MIN(cn.name) AS movie_company, MIN(mi_idx.info) AS rating, MIN(t.title) AS complete_euro_dark_movie FROM external('complete_cast.parquet') cc, external('comp_cast_type.parquet') cct1, external('comp_cast_type.parquet') cct2, external('company_name.parquet') cn, external('company_type.parquet') ct, external('info_type.parquet') it1, external('info_type.parquet') it2, external('keyword.parquet') k, external('kind_type.parquet') kt, external('movie_companies.parquet') mc, external('movie_info.parquet') mi, external('movie_info_idx.parquet') mi_idx, external('movie_keyword.parquet') mk, external('title.parquet') t WHERE cct1.kind  = 'crew' AND cct2.kind  != 'complete+verified' AND cn.country_code  != '[us]' AND it1.info  = 'countries' AND it2.info  = 'rating' AND k.keyword  in ('murder', 'murder-in-title', 'blood', 'violence') AND kt.kind  in ('movie', 'episode') AND mc.note  not like '%(USA)%' and mc.note like '%(200%)%' AND mi.info  IN ('Sweden', 'Germany', 'Swedish', 'German') AND mi_idx.info  > '6.5' AND t.production_year  > 2005 AND kt.id = t.kind_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mi_idx.movie_id AND t.id = mc.movie_id AND t.id = cc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mi_idx.movie_id AND mk.movie_id = mc.movie_id AND mk.movie_id = cc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mc.movie_id AND mi.movie_id = cc.movie_id AND mc.movie_id = mi_idx.movie_id AND mc.movie_id = cc.movie_id AND mi_idx.movie_id = cc.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id AND ct.id = mc.company_type_id AND cn.id = mc.company_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id;
