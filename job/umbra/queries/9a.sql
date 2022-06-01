SELECT MIN(an.name) AS alternative_name, MIN(chn.name) AS character_name, MIN(t.title) AS movie FROM umbra.parquetview('aka_name.parquet') AS an, umbra.parquetview('char_name.parquet') AS chn, umbra.parquetview('cast_info.parquet') AS ci, umbra.parquetview('company_name.parquet') AS cn, umbra.parquetview('movie_companies.parquet') AS mc, umbra.parquetview('name.parquet') AS n, umbra.parquetview('role_type.parquet') AS rt, umbra.parquetview('title.parquet') AS t WHERE ci.note  in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)') AND cn.country_code ='[us]' AND mc.note  is not NULL and (mc.note like '%(USA)%' or mc.note like '%(worldwide)%') AND n.gender ='f' and n.name like '%Ang%' AND rt.role ='actress' AND t.production_year  between 2005 and 2015 AND ci.movie_id = t.id AND t.id = mc.movie_id AND ci.movie_id = mc.movie_id AND mc.company_id = cn.id AND ci.role_id = rt.id AND n.id = ci.person_id AND chn.id = ci.person_role_id AND an.person_id = n.id AND an.person_id = ci.person_id;
