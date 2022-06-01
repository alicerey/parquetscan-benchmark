SELECT MIN(n.name) AS cast_member, MIN(t.title) AS complete_dynamic_hero_movie FROM umbra.parquetview('complete_cast.parquet') AS cc, umbra.parquetview('comp_cast_type.parquet') AS cct1, umbra.parquetview('comp_cast_type.parquet') AS cct2, umbra.parquetview('char_name.parquet') AS chn, umbra.parquetview('cast_info.parquet') AS ci, umbra.parquetview('keyword.parquet') AS k, umbra.parquetview('kind_type.parquet') AS kt, umbra.parquetview('movie_keyword.parquet') AS mk, umbra.parquetview('name.parquet') AS n, umbra.parquetview('title.parquet') AS t WHERE cct1.kind  = 'cast' AND cct2.kind  like '%complete%' AND chn.name  is not NULL and (chn.name like '%man%' or chn.name like '%Man%') AND k.keyword  in ('superhero', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence', 'magnet', 'web', 'claw', 'laser') AND kt.kind  = 'movie' AND t.production_year  > 2000 AND kt.id = t.kind_id AND t.id = mk.movie_id AND t.id = ci.movie_id AND t.id = cc.movie_id AND mk.movie_id = ci.movie_id AND mk.movie_id = cc.movie_id AND ci.movie_id = cc.movie_id AND chn.id = ci.person_role_id AND n.id = ci.person_id AND k.id = mk.keyword_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id;
