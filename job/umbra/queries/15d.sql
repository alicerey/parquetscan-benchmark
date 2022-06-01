SELECT MIN(at.title) AS aka_title, MIN(t.title) AS internet_movie_title
FROM
     umbra.parquetview('aka_title.parquet') AS at,
     umbra.parquetview('company_name.parquet') AS cn,
     umbra.parquetview('company_type.parquet') AS ct,
     umbra.parquetview('info_type.parquet') AS it1,
     umbra.parquetview('keyword.parquet') AS k,
     umbra.parquetview('movie_companies.parquet') AS mc,
     umbra.parquetview('movie_info.parquet') AS mi,
     umbra.parquetview('movie_keyword.parquet') AS mk,
     umbra.parquetview('title.parquet') AS t
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
