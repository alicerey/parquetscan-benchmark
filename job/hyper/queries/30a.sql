SELECT MIN(mi.info) AS movie_budget, MIN(mi_idx.info) AS movie_votes, MIN(n.name) AS writer, MIN(t.title) AS complete_violent_movie FROM external('complete_cast.parquet') cc, external('comp_cast_type.parquet') cct1, external('comp_cast_type.parquet') cct2, external('cast_info.parquet') ci, external('info_type.parquet') it1, external('info_type.parquet') it2, external('keyword.parquet') k, external('movie_info.parquet') mi, external('movie_info_idx.parquet') mi_idx, external('movie_keyword.parquet') mk, external('name.parquet') n, external('title.parquet') t WHERE cct1.kind  in ('cast', 'crew') AND cct2.kind  ='complete+verified' AND ci.note  in ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)') AND it1.info  = 'genres' AND it2.info  = 'votes' AND k.keyword  in ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital') AND mi.info  in ('Horror', 'Thriller') AND n.gender  = 'm' AND t.production_year  > 2000 AND t.id = mi.movie_id AND t.id = mi_idx.movie_id AND t.id = ci.movie_id AND t.id = mk.movie_id AND t.id = cc.movie_id AND ci.movie_id = mi.movie_id AND ci.movie_id = mi_idx.movie_id AND ci.movie_id = mk.movie_id AND ci.movie_id = cc.movie_id AND mi.movie_id = mi_idx.movie_id AND mi.movie_id = mk.movie_id AND mi.movie_id = cc.movie_id AND mi_idx.movie_id = mk.movie_id AND mi_idx.movie_id = cc.movie_id AND mk.movie_id = cc.movie_id AND n.id = ci.person_id AND it1.id = mi.info_type_id AND it2.id = mi_idx.info_type_id AND k.id = mk.keyword_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id;
