ALTER TABLE pins_mapuser 
ADD COLUMN q5_gender varchar;

 UPDATE pins_mapuser SET q5_gender = 
    CASE WHEN lower(q5) in ('female', 'f') then 'Female'
               WHEN lower(q5) in ('male', 'cisgender male') then 'Male' END;