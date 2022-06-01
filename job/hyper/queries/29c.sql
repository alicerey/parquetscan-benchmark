SELECT MIN(chn.name) AS voiced_char, MIN(n.name) AS voicing_actress, MIN(t.title) AS voiced_animation 
FROM external('aka_name.parquet') an,
     external('complete_cast.parquet') cc,
     external('comp_cast_type.parquet') cct1,
     external('comp_cast_type.parquet') cct2,
     external('char_name.parquet') chn,
     external('cast_info.parquet') ci,
     external('company_name.parquet') cn,
     external('info_type.parquet') it,
     external('info_type.parquet') it3,
     external('keyword.parquet') k,
     external('movie_companies.parquet') mc,
     external('movie_info.parquet') mi,
     external('movie_keyword.parquet') mk,
     external('name.parquet') n,
     external('person_info.parquet') pi,
     external('role_type.parquet') rt,
     external('title.parquet') t
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
