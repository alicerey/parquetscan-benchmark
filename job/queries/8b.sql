SELECT MIN(an.name) AS acress_pseudonym, MIN(t.title) AS japanese_anime_movie FROM 'aka_name.parquet' an, 'cast_info.parquet' ci, 'company_name.parquet' cn, 'movie_companies.parquet' mc, 'name.parquet' n, 'role_type.parquet' rt, 'title.parquet' t WHERE ci.note ='(voice: English version)' AND cn.country_code ='[jp]' AND mc.note like '%(Japan)%' and mc.note not like '%(USA)%' and (mc.note like '%(2006)%' or mc.note like '%(2007)%') AND n.name like '%Yo%' and n.name not like '%Yu%' AND rt.role ='actress' AND t.production_year between 2006 and 2007 and (t.title like 'One Piece%' or t.title like 'Dragon Ball Z%') AND an.person_id = n.id AND n.id = ci.person_id AND ci.movie_id = t.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND ci.role_id = rt.id AND an.person_id = ci.person_id AND ci.movie_id = mc.movie_id;
