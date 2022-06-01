SELECT MIN(chn.name) AS voiced_char_name, MIN(n.name) AS voicing_actress_name, MIN(t.title) AS kung_fu_panda FROM umbra.parquetview('aka_name.parquet') AS an, umbra.parquetview('char_name.parquet') AS chn, umbra.parquetview('cast_info.parquet') AS ci, umbra.parquetview('company_name.parquet') AS cn, umbra.parquetview('info_type.parquet') AS it, umbra.parquetview('keyword.parquet') AS k, umbra.parquetview('movie_companies.parquet') AS mc, umbra.parquetview('movie_info.parquet') AS mi, umbra.parquetview('movie_keyword.parquet') AS mk, umbra.parquetview('name.parquet') AS n, umbra.parquetview('role_type.parquet') AS rt, umbra.parquetview('title.parquet') AS t WHERE ci.note  in ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)') AND cn.country_code ='[us]' AND cn.name  = 'DreamWorks Animation' AND it.info  = 'release dates' AND k.keyword  in ('hero', 'martial-arts', 'hand-to-hand-combat', 'computer-animated-movie') AND mi.info  is not null and (mi.info like 'Japan:%201%' or mi.info like 'USA:%201%') AND n.gender ='f' and n.name like '%An%' AND rt.role ='actress' AND t.production_year  > 2010 AND t.title like 'Kung Fu Panda%' AND t.id = mi.movie_id AND t.id = mc.movie_id AND t.id = ci.movie_id AND t.id = mk.movie_id AND mc.movie_id = ci.movie_id AND mc.movie_id = mi.movie_id AND mc.movie_id = mk.movie_id AND mi.movie_id = ci.movie_id AND mi.movie_id = mk.movie_id AND ci.movie_id = mk.movie_id AND cn.id = mc.company_id AND it.id = mi.info_type_id AND n.id = ci.person_id AND rt.id = ci.role_id AND n.id = an.person_id AND ci.person_id = an.person_id AND chn.id = ci.person_role_id AND k.id = mk.keyword_id;
