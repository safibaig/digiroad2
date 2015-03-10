Insert into MANOEUVRE (ID,TYPE,MODIFIED_DATE,MODIFIED_BY) values (97666,2,to_timestamp('24-FEB-15','DD-MON-RR'),'dr1_conversion');
Insert into MANOEUVRE (ID,TYPE,MODIFIED_DATE,MODIFIED_BY) values (97665,2,to_timestamp('24-FEB-15','DD-MON-RR'),'dr1_conversion');
Insert into MANOEUVRE (ID,TYPE,MODIFIED_DATE,MODIFIED_BY) values (39560,2,to_timestamp('24-FEB-15','DD-MON-RR'),'dr1_conversion');
Insert into MANOEUVRE (ID,TYPE,MODIFIED_DATE,MODIFIED_BY) values (39559,2,to_timestamp('24-FEB-15','DD-MON-RR'),'dr1_conversion');
Insert into MANOEUVRE (ID,TYPE,MODIFIED_DATE,MODIFIED_BY) values (39564,2,to_timestamp('24-FEB-15','DD-MON-RR'),'dr1_conversion');
Insert into MANOEUVRE (ID,TYPE,MODIFIED_DATE,MODIFIED_BY) values (39561,2,to_timestamp('24-FEB-15','DD-MON-RR'),'dr1_conversion');
Insert into MANOEUVRE (ID,TYPE,MODIFIED_DATE,MODIFIED_BY) values (39558,2,to_timestamp('24-FEB-15','DD-MON-RR'),'dr1_conversion');
Insert into MANOEUVRE (ID,TYPE,MODIFIED_DATE,MODIFIED_BY) values (39563,2,to_timestamp('24-FEB-15','DD-MON-RR'),'dr1_conversion');
Insert into MANOEUVRE (ID,TYPE,MODIFIED_DATE,MODIFIED_BY) values (39562,2,to_timestamp('24-FEB-15','DD-MON-RR'),'dr1_conversion');

Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (97666,6586,3);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (97665,6683,3);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (97666,6674,1);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (97665,6674,1);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39560,6948,1);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39559,6868,3);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39564,7292,1);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39561,6725,3);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39558,7377,1);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39559,6585,1);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39560,6605,3);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39563,6907,1);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39558,7292,3);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39561,6846,1);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39562,6683,1);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39562,7389,3);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39564,6804,3);
Insert into MANOEUVRE_ELEMENT (ID,ROAD_LINK_ID,ELEMENT_TYPE) values (39563,6804,3);

-- Ajoneuvo sallittu poikkeussännöt:
-- 4 Kuorma-auto
-- 5 Linja-auto
-- 6 Pakettiauto
-- 7 Henkilöauto
-- 8 Taksi
-- 13 Ajoneuvoyhdistelmä
-- 14 Traktori tai maatalousajoneuvo
-- 15 Matkailuajoneuvo
-- 16 Jakeluauto
-- 18 Kimppakyytiajoneuvo
-- 19 Sotilasajoneuvo
-- 20 Vaarallista lastia kuljettava ajoneuvo
-- 21 Huoltoajo
-- 22 Tontille ajo

Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (97666, 4);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (97666, 5);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (97665, 8);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (97665, 16);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (97665, 18);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (97665, 19);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (39558, 21);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (39559, 21);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (39560, 21);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (39561, 19);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (39561, 21);
Insert into MANOEUVRE_EXCEPTIONS (MANOEUVRE_ID, EXCEPTION_TYPE) values (39561, 22);

