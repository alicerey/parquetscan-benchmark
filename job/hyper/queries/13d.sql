SELECT MIN(cn.name) AS producing_company, MIN(miidx.info) AS rating, MIN(t.title) AS movie FROM external('company_name.parquet') cn, external('company_type.parquet') ct, external('info_type.parquet') it, external('info_type.parquet') it2, external('kind_type.parquet') kt, external('movie_companies.parquet') mc, external('movie_info.parquet') mi, external('movie_info_idx.parquet') miidx, external('title.parquet') t WHERE cn.country_code ='[us]' AND ct.kind ='production companies' AND it.info ='rating' AND it2.info ='release dates' AND kt.kind ='movie' AND mi.movie_id = t.id AND it2.id = mi.info_type_id AND kt.id = t.kind_id AND mc.movie_id = t.id AND cn.id = mc.company_id AND ct.id = mc.company_type_id AND miidx.movie_id = t.id AND it.id = miidx.info_type_id AND mi.movie_id = miidx.movie_id AND mi.movie_id = mc.movie_id AND miidx.movie_id = mc.movie_id;
