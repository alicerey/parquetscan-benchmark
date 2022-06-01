SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS writer, MIN(t.title) AS violent_liongate_movie FROM umbra.parquetview('cast_info.parquet') AS ci, umbra.parquetview('company_name.parquet') AS cn, umbra.parquetview('info_type.parquet') AS it1, umbra.parquetview('info_type.parquet') AS it2, umbra.parquetview('keyword.parquet') AS k, umbra.parquetview('movie_companies.parquet') AS mc, umbra.parquetview('movie_info.parquet') AS mi, umbra.parquetview('movie_info_idx.parquet') AS mi_idx, umbra.parquetview('movie_keyword.parquet') AS mk, umbra.parquetview('name.parquet') AS n, umbra.parquetview('title.parquet') AS t WHERE ci.note  in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)') AND cn.name  like 'Lionsgate%' AND it1.info  = 'genres' AND it2.info  = 'votes' AND k.keyword  in ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital') AND mi.info  in ('Horror', 'Thriller') AND n.gender   = 'm' AND t.id = mi.movie_id AND t.id = mi_idx.movie_id AND t.id = ci.movie_id AND t.id = mk.movie_id AND t.id = mc.movie_id AND ci.movie_id = mi.movie_id AND ci.movie_id = mi_idx.movie_id AND ci.movie_id = mk.movie_id AND ci.movie_id = mc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mk.movie_id AND mi.movie_id = mc.movie_id AND mi_idx.movie_id = mk.movie_id AND mi_idx.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id AND n.id = ci.person_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id AND k.id = mk.keyword_id AND cn.id = mc.company_id;
