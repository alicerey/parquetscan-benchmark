SELECT MIN(n.name) AS of_person, MIN(t.title) AS biography_movie
FROM 'aka_name.parquet' an,
     'cast_info.parquet' ci,
     'info_type.parquet' it,
     'link_type.parquet' lt,
     'movie_link.parquet' ml,
     'name.parquet' n,
     'person_info.parquet' pi,
     'title.parquet' t
WHERE an.name LIKE '%a%' AND it.info ='mini biography' AND
      lt.link ='features' AND n.name_pcode_cf BETWEEN 'A' AND 'F' AND
      (n.gender='m' OR (n.gender = 'f' AND n.name LIKE 'B%')) AND pi.note ='Volker Boehm' AND
      t.production_year BETWEEN 1980 AND 1995 AND n.id = an.person_id AND
      n.id = pi.person_id AND ci.person_id = n.id AND t.id = ci.movie_id AND
      ml.linked_movie_id = t.id AND lt.id = ml.link_type_id AND it.id = pi.info_type_id AND
      pi.person_id = an.person_id AND pi.person_id = ci.person_id AND an.person_id = ci.person_id AND ci.movie_id = ml.linked_movie_id;
