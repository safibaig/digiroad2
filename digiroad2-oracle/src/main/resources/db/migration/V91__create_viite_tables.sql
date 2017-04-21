CREATE TABLE PROJECT
(
  ID NUMBER NOT NULL
, STATE NUMBER DEFAULT 0 NOT NULL
, NAME VARCHAR2(32) NOT NULL
, ELY NUMBER NOT NULL
, CREATED_BY VARCHAR2(32) NOT NULL
, CREATED_DATE DATE DEFAULT SYSDATE NOT NULL
, MODIFIED_BY VARCHAR2(32)
, MODIFIED_DATE DATE
, CONSTRAINT PROJECT_PK PRIMARY KEY
  (
    ID
  )
  ENABLE
);

CREATE TABLE PROJECT_LINK
(
  ID NUMBER NOT NULL
, PROJECT_ID NUMBER NOT NULL
, ROAD_TYPE NUMBER NOT NULL
, DISCONTINUITY_TYPE NUMBER DEFAULT 5 NOT NULL
, ROAD_NUMBER NUMBER
, ROAD_PART_NUMBER NUMBER
, START_ADDR_M NUMBER
, END_ADDR_M NUMBER
, LRM_POSITION_ID NUMBER NOT NULL
, CREATED_BY VARCHAR2(32) NOT NULL
, MODIFIED_BY VARCHAR2(32)
, CREATED_DATE DATE DEFAULT SYSDATE NOT NULL
, MODIFIED_DATE DATE
, CONSTRAINT PROJECT_LINK_PK PRIMARY KEY
  (
    ID
  )
  ENABLE
);

CREATE INDEX PROJECT_LINK_PRJ_IDX ON PROJECT_LINK (PROJECT_ID);

ALTER TABLE PROJECT_LINK
ADD CONSTRAINT PROJECT_LINK_PROJECT_ID_FK1 FOREIGN KEY
(
  PROJECT_ID
)
REFERENCES PROJECT
(
  ID
)
ON DELETE CASCADE ENABLE;

ALTER TABLE PROJECT_LINK
ADD CONSTRAINT PROJECT_LINK_POSITION_FK2 FOREIGN KEY
(
  LRM_POSITION_ID
)
REFERENCES LRM_POSITION
(
  ID
)
ENABLE;


ALTER TABLE PROJECT_LINK
ADD CONSTRAINT PROJECT_LINK_ADDRESS_CHK1 CHECK
((road_number is null and road_part_number is null) or
(road_number is not null and road_part_number is not null and start_addr_m is not null and end_addr_m is not null))
ENABLE;

CREATE TABLE CALIBRATION_POINT
(
  PROJECT_LINK_ID NUMBER NOT NULL
, LINK_M NUMERIC NOT NULL
, ADDR_M NUMBER NOT NULL
);

CREATE INDEX CALIBRATION_POINT_PRJLINK_ID ON CALIBRATION_POINT (PROJECT_LINK_ID);

ALTER TABLE CALIBRATION_POINT
ADD CONSTRAINT CALIBRATION_POINT_FK1 FOREIGN KEY
(
  PROJECT_LINK_ID
)
REFERENCES PROJECT_LINK
(
  ID
)
ON DELETE CASCADE ENABLE;

CREATE TABLE ROAD_ADDRESS
(
  ID NUMBER NOT NULL
, ROAD_NUMBER NUMBER NOT NULL
, ROAD_PART_NUMBER NUMBER NOT NULL
, TRACK_CODE NUMBER NOT NULL
, ELY NUMBER NOT NULL
, ROAD_TYPE NUMBER NOT NULL
, DISCONTINUITY NUMBER NOT NULL
, START_ADDR_M NUMBER NOT NULL
, END_ADDR_M NUMBER NOT NULL
, LRM_POSITION_ID NUMBER NOT NULL
, START_DATE DATE NOT NULL
, END_DATE DATE
, CREATED_BY VARCHAR2(32) NOT NULL
, CREATED_DATE DATE DEFAULT sysdate NOT NULL
, CONSTRAINT ROAD_ADDRESS_PK PRIMARY KEY
  (
    ID
  )
  ENABLE
);

ALTER TABLE ROAD_ADDRESS
ADD CONSTRAINT ROAD_ADDRESS_FK1 FOREIGN KEY
(
  LRM_POSITION_ID
)
REFERENCES LRM_POSITION
(
  ID
)
ENABLE;

CREATE INDEX ROAD_ADDRESS_ADDR ON ROAD_ADDRESS (ROAD_NUMBER, ROAD_PART_NUMBER, TRACK_CODE);
CREATE UNIQUE INDEX ROAD_ADDRESS_LRM ON ROAD_ADDRESS (LRM_POSITION_ID);

CREATE TABLE ROAD_ADDRESS_CHANGES
(
  PROJECT_ID NUMBER NOT NULL
, CHANGE_TYPE NUMBER NOT NULL
, OLD_ROAD_NUMBER NUMBER NOT NULL
, NEW_ROAD_NUMBER NUMBER NOT NULL
, OLD_ROAD_PART_NUMBER NUMBER NOT NULL
, NEW_ROAD_PART_NUMBER NUMBER NOT NULL
, OLD_TRACK_CODE NUMBER NOT NULL
, NEW_TRACK_CODE NUMBER NOT NULL
, OLD_START_ADDR_M NUMBER NOT NULL
, NEW_START_ADDR_M NUMBER NOT NULL
, OLD_END_ADDR_M NUMBER NOT NULL
, NEW_END_ADDR_M NUMBER NOT NULL
, NEW_DISCONTINUITY NUMBER NOT NULL
, NEW_ROAD_TYPE NUMBER NOT NULL
, NEW_ELY NUMBER NOT NULL
);

-- For project ids only
CREATE SEQUENCE VIITE_PROJECT_SEQ INCREMENT BY 1 START WITH 1 MAXVALUE 99999999 MINVALUE 1 NOCACHE ORDER;

-- For project_link, road_address tables
CREATE SEQUENCE VIITE_GENERAL_SEQ INCREMENT BY 1 START WITH 1 MAXVALUE 9999999999 MINVALUE 1 CACHE 20;


