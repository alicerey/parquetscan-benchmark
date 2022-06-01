SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(t.title) AS movie_title FROM external('cast_info.parquet') ci, external('info_type.parquet') it1, external('info_type.parquet') it2, external('movie_info.parquet') mi, external('movie_info_idx.parquet') mi_idx, external('name.parquet') n, external('title.parquet') t WHERE ci.note  in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)') AND it1.info  = 'genres' AND it2.info  = 'rating' AND mi.info  in ('Horror', 'Thriller') and mi.note is NULL AND mi_idx.info  > '8.0' AND n.gender  is not null and n.gender = 'f' AND t.production_year  between 2008 and 2014 AND t.id = mi.movie_id AND t.id = mi_idx.movie_id AND t.id = ci.movie_id AND ci.movie_id = mi.movie_id AND ci.movie_id = mi_idx.movie_id AND mi.movie_id = mi_idx.movie_id AND n.id = ci.person_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id;
