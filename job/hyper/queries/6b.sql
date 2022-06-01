SELECT MIN(k.keyword) AS movie_keyword, MIN(n.name) AS actor_name, MIN(t.title) AS hero_movie FROM external('cast_info.parquet') ci, external('keyword.parquet') k, external('movie_keyword.parquet') mk, external('name.parquet') n, external('title.parquet') t WHERE k.keyword in ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence') AND n.name LIKE '%Downey%Robert%' AND t.production_year > 2014 AND k.id = mk.keyword_id AND t.id = mk.movie_id AND t.id = ci.movie_id AND ci.movie_id = mk.movie_id AND n.id = ci.person_id;
