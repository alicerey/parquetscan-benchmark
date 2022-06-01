SELECT MIN(n.name) AS of_person, MIN(t.title) AS biography_movie
FROM external('aka_name.parquet') an,
     external('cast_info.parquet') ci,
     external('info_type.parquet') it,
     external('link_type.parquet') lt,
     external('movie_link.parquet') ml,
     external('name.parquet') n,
     external('person_info.parquet') pi,
     external('title.parquet') t
WHERE an.name LIKE '%a%' AND it.info ='mini biography' AND
      lt.link ='features' AND n.name_pcode_cf BETWEEN 'A' AND 'F' AND
      (n.gender='m' OR (n.gender = 'f' AND n.name LIKE 'B%')) AND pi.note ='Volker Boehm' AND
      t.production_year BETWEEN 1980 AND 1995 AND n.id = an.person_id AND
      n.id = pi.person_id AND ci.person_id = n.id AND t.id = ci.movie_id AND
      ml.linked_movie_id = t.id AND lt.id = ml.link_type_id AND it.id = pi.info_type_id AND
      pi.person_id = an.person_id AND pi.person_id = ci.person_id AND an.person_id = ci.person_id AND ci.movie_id = ml.linked_movie_id;
