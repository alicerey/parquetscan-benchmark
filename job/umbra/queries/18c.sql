SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(t.title) AS movie_title FROM umbra.parquetview('cast_info.parquet') AS ci, umbra.parquetview('info_type.parquet') AS it1, umbra.parquetview('info_type.parquet') AS it2, umbra.parquetview('movie_info.parquet') AS mi, umbra.parquetview('movie_info_idx.parquet') AS mi_idx, umbra.parquetview('name.parquet') AS n, umbra.parquetview('title.parquet') AS t WHERE ci.note  in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)') AND it1.info  = 'genres' AND it2.info  = 'votes' AND mi.info  in ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War') AND n.gender  = 'm' AND t.id = mi.movie_id AND t.id = mi_idx.movie_id AND t.id = ci.movie_id AND ci.movie_id = mi.movie_id AND ci.movie_id = mi_idx.movie_id AND mi.movie_id = mi_idx.movie_id AND n.id = ci.person_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id;