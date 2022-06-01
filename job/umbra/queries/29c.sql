SELECT MIN(chn.name) AS voiced_char, MIN(n.name) AS voicing_actress, MIN(t.title) AS voiced_animation 
FROM umbra.parquetview('aka_name.parquet') AS an,
     umbra.parquetview('complete_cast.parquet') AS cc,
     umbra.parquetview('comp_cast_type.parquet') AS cct1,
     umbra.parquetview('comp_cast_type.parquet') AS cct2,
     umbra.parquetview('char_name.parquet') AS chn,
     umbra.parquetview('cast_info.parquet') AS ci,
     umbra.parquetview('company_name.parquet') AS cn,
     umbra.parquetview('info_type.parquet') AS it,
     umbra.parquetview('info_type.parquet') AS it3,
     umbra.parquetview('keyword.parquet') AS k,
     umbra.parquetview('movie_companies.parquet') AS mc,
     umbra.parquetview('movie_info.parquet') AS mi,
     umbra.parquetview('movie_keyword.parquet') AS mk,
     umbra.parquetview('name.parquet') AS n,
     umbra.parquetview('person_info.parquet') AS pi,
     umbra.parquetview('role_type.parquet') AS rt,
     umbra.parquetview('title.parquet') AS t
WHERE cct1.kind  ='cast'  
  AND cct2.kind  ='complete+verified'  
  AND ci.note  in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')  
  AND cn.country_code ='[us]'  
  AND it.info  = 'release dates'  
  AND it3.info  = 'trivia'  
  AND k.keyword  = 'computer-animation'  
  AND mi.info  is not null  
  AND (mi.info like 'Japan:%200%' or mi.info like 'USA:%200%')  
  AND n.gender ='f'  
  AND n.name like '%An%'  
  AND rt.role ='actress'  
  AND t.production_year  between 2000  AND 2010
  AND t.id = mi.movie_id  
  AND t.id = mc.movie_id  
  AND t.id = ci.movie_id  
  AND t.id = mk.movie_id  
  AND t.id = cc.movie_id  
  AND mc.movie_id = ci.movie_id  
  AND mc.movie_id = mi.movie_id  
  AND mc.movie_id = mk.movie_id  
  AND mc.movie_id = cc.movie_id  
  AND mi.movie_id = ci.movie_id  
  AND mi.movie_id = mk.movie_id  
  AND mi.movie_id = cc.movie_id  
  AND ci.movie_id = mk.movie_id  
  AND ci.movie_id = cc.movie_id  
  AND mk.movie_id = cc.movie_id  
  AND cn.id = mc.company_id  
  AND it.id = mi.info_type_id  
  AND n.id = ci.person_id  
  AND rt.id = ci.role_id  
  AND n.id = an.person_id  
  AND ci.person_id = an.person_id  
  AND chn.id = ci.person_role_id  
  AND n.id = pi.person_id  
  AND ci.person_id = pi.person_id  
  AND it3.id = pi.info_type_id  
  AND k.id = mk.keyword_id  
  AND cct1.id = cc.subject_id  
  AND cct2.id = cc.status_id;
