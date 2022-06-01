SELECT MIN(an.name) AS cool_actor_pseudonym, MIN(t.title) AS series_named_after_char FROM 'aka_name.parquet' an, 'cast_info.parquet' ci, 'company_name.parquet' cn, 'keyword.parquet' k, 'movie_companies.parquet' mc, 'movie_keyword.parquet' mk, 'name.parquet' n, 'title.parquet' t WHERE cn.country_code ='[us]' AND k.keyword ='character-name-in-title' AND an.person_id = n.id AND n.id = ci.person_id AND ci.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND an.person_id = ci.person_id AND ci.movie_id = mc.movie_id AND ci.movie_id = mk.movie_id AND mc.movie_id = mk.movie_id;