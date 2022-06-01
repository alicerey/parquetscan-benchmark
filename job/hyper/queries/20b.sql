SELECT MIN(t.title) AS complete_downey_ironman_movie FROM external('complete_cast.parquet') cc, external('comp_cast_type.parquet') cct1, external('comp_cast_type.parquet') cct2, external('char_name.parquet') chn, external('cast_info.parquet') ci, external('keyword.parquet') k, external('kind_type.parquet') kt, external('movie_keyword.parquet') mk, external('name.parquet') n, external('title.parquet') t WHERE cct1.kind  = 'cast' AND cct2.kind  like '%complete%' AND chn.name  not like '%Sherlock%' and (chn.name like '%Tony%Stark%' or chn.name like '%Iron%Man%') AND k.keyword  in ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence') AND kt.kind  = 'movie' AND n.name  LIKE '%Downey%Robert%' AND t.production_year  > 2000 AND kt.id = t.kind_id AND t.id = mk.movie_id AND t.id = ci.movie_id AND t.id = cc.movie_id AND mk.movie_id = ci.movie_id AND mk.movie_id = cc.movie_id AND ci.movie_id = cc.movie_id AND chn.id = ci.person_role_id AND n.id = ci.person_id AND k.id = mk.keyword_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id;
