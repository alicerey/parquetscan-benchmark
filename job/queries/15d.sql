SELECT MIN(at.title) AS aka_title, MIN(t.title) AS internet_movie_title
FROM
     'aka_title.parquet' at,
     'company_name.parquet' cn,
     'company_type.parquet' ct,
     'info_type.parquet' it1,
     'keyword.parquet' k,
     'movie_companies.parquet' mc,
     'movie_info.parquet' mi,
     'movie_keyword.parquet' mk,
     'title.parquet' t
WHERE
      cn.country_code  = '[us]'
  AND it1.info  = 'release dates'
  AND mi.note  like '%internet%'
  AND t.production_year  > 1990
  AND t.id = at.movie_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = at.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = at.movie_id
  AND mc.movie_id = at.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;
