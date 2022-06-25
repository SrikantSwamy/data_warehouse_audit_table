

CREATE or REPLACE PROCEDURE PR_AUDITLOG(in_table_name IN VARCHAR2)
AUTHID CURRENT_USER
IS

/*

********* Description: Procedure to create audit table and audit trigger ******
****** Input Parameters: Table name for which auditing has to be enabled ******
****** Author: Srikant S

*/

lv_src_table_nm VARCHAR2(255);
gv_audit_tab_pfx VARCHAR2(255);
lv_audit_tab_nm VARCHAR2(255);
lv_aud_tab_dyn VARCHAR2(1000);
lv_aud_tab_alt_dyn VARCHAR2(1000);

gv_audit_trig_pfx VARCHAR2(255);
lv_audit_trig_nm VARCHAR2(255);

lv_trig_start_sytx VARCHAR2(20000);

ln_step NUMBER;
lv_proc_name VARCHAR2(255);
lv_err_code NUMBER;
lv_err_msg VARCHAR2(4000);

ln_obj_cnt NUMBER;

--Cursor to read the columns of the audit table

CURSOR cur_aud_tab_col(p_aud_tbl_name user_tables.table_name%TYPE) IS
SELECT column_name,data_type,data_length,data_precision,data_scale
FROM user_tab_columns
WHERE UPPER(table_name)=p_aud_tbl_name
AND column_name NOT IN ('AUDIT_INS_DATE')
ORDER BY column_id;

--Cursor to remove the not null constraints which are present in the audit table

CURSOR cur_rem_aud_tab_cons(p_aud_tbl_name user_tables.table_name%TYPE) IS
SELECT table_name,constraint_name FROM user_constraints 
WHERE UPPER(table_name)=p_aud_tbl_name
AND constraint_type='C'
AND search_condition IS NOT NULL;

BEGIN

-- ****************** Main Processing Begins ******************
ln_step:=10;

lv_proc_name :='PR_AUDIT_LOG';
gv_audit_tab_pfx:='AUD';
gv_audit_trig_pfx:='TR';

lv_src_table_nm:=UPPER(in_table_name);

--To check if the source table exists

SELECT COUNT(1) 
INTO ln_obj_cnt 
FROM user_tables 
WHERE UPPER(table_name)=lv_src_table_nm;


ln_step:=15;

--Raise exception if table does not exists

IF ln_obj_cnt=0
THEN
RAISE_APPLICATION_ERROR(-20111,'Table name '||lv_src_table_nm||' is invalid');
END IF;

lv_audit_tab_nm:=gv_audit_tab_pfx||'_'||lv_src_table_nm;

--To ensure the length of the table name is within limit

IF LENGTH(lv_audit_tab_nm)>30 THEN
lv_audit_tab_nm:=SUBSTR(lv_audit_tab_nm,1,30);
DBMS_OUTPUT.PUT_LINE('******* The audit table identifier is trimmed to '||lv_audit_tab_nm||' *******');
END IF;

SELECT COUNT(1) 
INTO ln_obj_cnt 
FROM user_tables 
WHERE UPPER(table_name)=lv_audit_tab_nm;

--Raise exception if audit table already exists

IF ln_obj_cnt>0
THEN
RAISE_APPLICATION_ERROR(-20112,'Audit table '||lv_audit_tab_nm||' already exists');
END IF;

-- *********** Audit Table Creation Starts ***********
ln_step:=20;

lv_aud_tab_dyn:='CREATE TABLE '||lv_audit_tab_nm||' AS SELECT * FROM '||lv_src_table_nm||' WHERE 1>2';
EXECUTE IMMEDIATE lv_aud_tab_dyn;

ln_step:=25;

lv_aud_tab_alt_dyn:='ALTER TABLE '||lv_audit_tab_nm||' ADD AUDIT_INS_DATE DATE';

EXECUTE IMMEDIATE lv_aud_tab_alt_dyn;

DBMS_OUTPUT.PUT_LINE('******* Audit table '||lv_audit_tab_nm||' created for '||lv_src_table_nm||' *******');

--Process to remove the not null type constraints starts

FOR rem_aud_tab_cons_rec IN cur_rem_aud_tab_cons(lv_audit_tab_nm)
LOOP
EXECUTE IMMEDIATE 'ALTER TABLE '||rem_aud_tab_cons_rec.table_name||' DROP CONSTRAINT '
||rem_aud_tab_cons_rec.constraint_name;
END LOOP;

DBMS_OUTPUT.PUT_LINE('******* Not Null constraints of table '||lv_audit_tab_nm||' dropped *******');

-- ************ Audit Table Creation Ends **************

-- ************ Trigger Creation Starts *****************

lv_audit_trig_nm:=gv_audit_trig_pfx||'_'||lv_src_table_nm;

--To ensure the length of the trigger name is within limit

IF LENGTH(lv_audit_trig_nm)>30 THEN
lv_audit_trig_nm:=SUBSTR(lv_audit_trig_nm,1,30);
DBMS_OUTPUT.PUT_LINE('******* The audit trigger identifier is trimmed to '||lv_audit_trig_nm||' *******');
END IF;

SELECT COUNT(1) 
INTO ln_obj_cnt 
FROM user_triggers
WHERE UPPER(trigger_name)=lv_audit_trig_nm;

--Raise exception if trigger already exists

IF ln_obj_cnt>0
THEN
RAISE_APPLICATION_ERROR(-20113,'Trigger '||lv_audit_trig_nm||' already exists');
END IF;

lv_trig_start_sytx:='CREATE OR REPLACE TRIGGER '
||lv_audit_trig_nm||CHR(10)
||'AFTER UPDATE '
||'ON '
||lv_src_table_nm
||' FOR EACH ROW'||CHR(10)
||'BEGIN'||CHR(10)
||'IF UPDATING THEN'||CHR(10)
||'INSERT INTO '
||lv_audit_tab_nm||CHR(10)
||'('||CHR(10);

ln_step:=30;

FOR cur_aud_rec IN cur_aud_tab_col(lv_audit_tab_nm)
LOOP

lv_trig_start_sytx:=lv_trig_start_sytx||cur_aud_rec.column_name||','||CHR(10);

END LOOP;

ln_step:=35;

lv_trig_start_sytx:=lv_trig_start_sytx
||'AUDIT_INS_DATE'||CHR(10)
||')'||CHR(10)
||'VALUES'||CHR(10)
||'('||CHR(10);

FOR cur_aud_rec IN cur_aud_tab_col(lv_audit_tab_nm)
LOOP

lv_trig_start_sytx:=lv_trig_start_sytx||':old.'||cur_aud_rec.column_name||','||CHR(10);

END LOOP;

ln_step:=40;

lv_trig_start_sytx:=lv_trig_start_sytx
--||':new.UPD_DATE'||CHR(10)
||'SYSDATE'||CHR(10)
||');'||CHR(10)||CHR(10)
||'END IF;'||CHR(10)
||'END;';

ln_step:=50;
-- ************ Trigger Creation Ends *****************

--DBMS_OUTPUT.PUT_LINE(lv_trig_start_sytx);
EXECUTE IMMEDIATE lv_trig_start_sytx;

ln_step:=55;

DBMS_OUTPUT.PUT_LINE('******* Audit Trigger '||lv_audit_trig_nm||' created for '||lv_src_table_nm||' *******');
DBMS_OUTPUT.PUT_LINE('******* Auditing  has been enabled for '||lv_src_table_nm||' *******');

-- ****************** Main Processing Ends ******************

EXCEPTION
WHEN OTHERS THEN
lv_err_msg:=SQLERRM;
pr_record_error(lv_err_msg,lv_proc_name,ln_step);
DBMS_OUTPUT.PUT_LINE('********** Errors encountered, check error table **************');

END;

