SELECT MIN(mc.note) AS production_note, MIN(t.title) AS movie_title, MIN(t.production_year) AS movie_year FROM 'company_type.parquet' ct, 'info_type.parquet' it, 'movie_companies.parquet' mc, 'movie_info_idx.parquet' mi_idx, 'title.parquet' t WHERE ct.kind = 'production companies' AND it.info = 'bottom 10 rank' AND mc.note  not like '%(as Metro-Goldwyn-Mayer Pictures)%' AND t.production_year >2000 AND ct.id = mc.company_type_id AND t.id = mc.movie_id AND t.id = mi_idx.movie_id AND mc.movie_id = mi_idx.movie_id AND it.id = mi_idx.info_type_id;