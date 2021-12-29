ALTER TABLE pins_mapuser 
ADD COLUMN q5_gender varchar;

UPDATE pins_mapuser SET q5_gender = 'Male' WHERE q5 = 'Male';

UPDATE pins_mapuser SET q5_gender = 'Female' WHERE q5 = 'Female';

UPDATE pins_mapuser SET q5_gender = 'Male' WHERE q5 = 'male';

UPDATE pins_mapuser SET q5_gender = 'Female' WHERE q5 = 'female';

UPDATE pins_mapuser SET q5_gender = 'Male' WHERE q5 = 'cisgender male';

UPDATE pins_mapuser SET q5_gender = 'Female' WHERE q5 = 'F';