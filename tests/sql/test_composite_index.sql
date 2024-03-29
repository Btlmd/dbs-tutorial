USE DATASET;

DESC LINEITEM;

SELECT COUNT(*) FROM LINEITEM WHERE LINEITEM.L_SUPPKEY < 3000 AND LINEITEM.L_PARTKEY < 20;
-- composite index: 0.00 sec;

ALTER TABLE LINEITEM DROP FOREIGN KEY FK_1_2_REF_5_0_1_;

SELECT COUNT(*) FROM LINEITEM WHERE LINEITEM.L_SUPPKEY < 3000 AND LINEITEM.L_PARTKEY < 20;
-- no index 2.53 sec;

ALTER TABLE LINEITEM ADD INDEX (L_PARTKEY);
ALTER TABLE LINEITEM ADD INDEX (L_SUPPKEY);

SELECT COUNT(*) FROM LINEITEM WHERE LINEITEM.L_SUPPKEY < 3000 AND LINEITEM.L_PARTKEY < 20;
-- two single indexes 7.33 sec;