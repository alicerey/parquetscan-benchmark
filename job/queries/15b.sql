SELECT MIN(mi.info) AS release_date, MIN(t.title) AS youtube_movie FROM 'aka_title.parquet' at, 'company_name.parquet' cn, 'company_type.parquet' ct, 'info_type.parquet' it1, 'keyword.parquet' k, 'movie_companies.parquet' mc, 'movie_info.parquet' mi, 'movie_keyword.parquet' mk, 'title.parquet' t WHERE cn.country_code  = '[us]' and cn.name = 'YouTube' AND it1.info  = 'release dates' AND mc.note  like '%(200%)%' and mc.note like '%(worldwide)%' AND mi.note  like '%internet%' AND mi.info  like 'USA:% 200%' AND t.production_year  between 2005 and 2010 AND t.id = at.movie_id AND t.id = mi.movie_id AND t.id = mk.movie_id AND t.id = mc.movie_id AND mk.movie_id = mi.movie_id AND mk.movie_id = mc.movie_id AND mk.movie_id = at.movie_id AND mi.movie_id = mc.movie_id AND mi.movie_id = at.movie_id AND mc.movie_id = at.movie_id AND k.id = mk.keyword_id AND it1.id = mi.info_type_id AND cn.id = mc.company_id AND ct.id = mc.company_type_id;
