""" MyQueries.py """

from MyConstants import *

# QUERIES FOR FINDING TABLES.

not_implemented = "FINDING YOUR {} NOT IMPLEMENTED FOR {}."
not_possible_sql = "SQL CANNOT READ THE SCHEMA IN {} THROUGH {}."

find_tables_sql = dict()
find_tables_sql[access] = not_possible_sql
find_tables_sql[mysql] = (
    "SELECT table_name "
    "FROM information_schema.tables "
    "WHERE table_type = 'BASE TABLE' "
    "AND table_schema = database() "
    "ORDER BY table_name;")
find_tables_sql[oracle] = (
    "SELECT table_name "
    "FROM user_tables "
    "ORDER BY table_name")
find_tables_sql[postgresql] = (
    "SELECT table_name "
    "FROM information_schema.tables "
    "WHERE table_type = 'BASE TABLE' "
    "AND table_schema = 'public' "
    "ORDER BY table_name;")
find_tables_sql[sqlite] = (
    "SELECT name AS table_name "
    "FROM sqlite_master "
    "WHERE type='table' "
    "AND name NOT LIKE 'sqlite_%' "
    "ORDER BY name")
find_tables_sql[sqlserver] = (
    "SELECT name AS table_name "
    "FROM sys.tables "
    "WHERE type='U'"
    "ORDER BY name")

# QUERIES FOR FINDING VIEWS.

find_views_sql = dict()
find_views_sql[access] = not_possible_sql
find_views_sql[mysql] = (
    "SELECT table_name AS view_name, view_definition AS view_sql, "
    "  check_option, is_updatable "
    "FROM information_schema.views "
    "WHERE table_schema = database() "
    "ORDER BY table_name;")
find_views_sql[oracle] = (
    "SELECT view_name, text AS view_sql "
    "FROM user_views "
    "ORDER BY view_name")
find_views_sql[postgresql] = (
    "SELECT table_name AS view_name, view_definition AS view_sql, "
    "  check_option, is_updatable, is_insertable_into, "
    "  is_trigger_insertable_into, is_trigger_updatable, is_trigger_deletable "
    "FROM information_schema.views "
    "WHERE table_schema ='public' "
    "ORDER BY table_name;")
find_views_sql[sqlite] = (
    "SELECT name AS view_name, sql AS view_sql "
    "FROM sqlite_master "
    "WHERE type='view' "
    "ORDER BY name")
find_views_sql[sqlserver] = (
    "SELECT name AS view_name, object_definition(object_id(name)) AS view_sql "
    "FROM sys.views WHERE type='V' "
    "ORDER BY name")

# QUERIES FOR FINDING TABLE COLUMNS.

find_tab_col_sql = dict()
find_tab_col_sql[access] = not_possible_sql
find_tab_col_sql[mysql] = not_implemented
find_tab_col_sql[oracle] = (
    "SELECT column_id, c.column_name, "
    "  CASE "
    "    WHEN (data_type LIKE '%CHAR%' OR data_type IN ('RAW','UROWID')) "
    "      THEN data_type||'('||c.char_length|| "
    "           DECODE(char_used,'B',' BYTE','C',' CHAR',NULL)||')' "
    "    WHEN data_type = 'NUMBER' "
    "      THEN "
    "        CASE "
    "          WHEN c.data_precision IS NULL AND c.data_scale IS NULL "
    "            THEN 'NUMBER' "
    "          WHEN c.data_precision IS NULL AND c.data_scale IS NOT NULL "
    "            THEN 'NUMBER(38,'||c.data_scale||')' "
    "          ELSE data_type||'('||c.data_precision||','||c.data_scale||')' "
    "          END "
    "    WHEN data_type = 'BFILE' "
    "      THEN 'BINARY FILE LOB (BFILE)' "
    "    WHEN data_type = 'FLOAT' "
    "      THEN data_type||'('||to_char(data_precision)||')'|| "
    "           DECODE(data_precision, 126,' (double precision)', "
    "           63,' (real)',NULL) "
    "    ELSE data_type "
    "    END AS data_type, "
    "    DECODE(nullable,'Y','Yes','No') AS nullable, "
    "    data_default, "
    "    NVL(comments,'(null)') AS comments "
    "FROM user_tab_cols c, user_col_comments com "
    "WHERE c.table_name = '{}' "
    "AND c.table_name = com.table_name "
    "AND c.column_name = com.column_name "
    "ORDER BY column_id")
find_tab_col_sql[postgresql] = not_implemented
find_tab_col_sql[sqlite] = (
    "SELECT cid AS column_id, name AS column_name, type AS data_type, "
    "  CASE "
    '    WHEN "notnull" = 1 '
    "      THEN 'No' "
    "  ELSE 'Yes' "
    "  END AS nullable, "
    "  CASE "
    "    WHEN dflt_value IS NULL "
    "      THEN '(null)' "
    "    ELSE '' "
    "    END AS data_default "
    "FROM pragma_table_info('{}')")
# TODO NOT DONE YET
find_tab_col_sql[sqlserver] = (
    "SELECT c.column_id, c.name AS column_name, "
    "    CASE "
    "        WHEN t.name = 'varchar' "
    "            THEN CONCAT(t.name,'(',c.max_length,')') "
    "        WHEN t.name LIKE '%INT' "
    "            THEN t.name "
    "        WHEN t.name IN ('money','datetime') "
    "            THEN t.name "
    "        ELSE CONCAT(t.name,',',c.precision,',',c.scale) "
    "        END AS data_type, "
    "    CASE "
    "        WHEN c.is_identity = 1 "
    "            THEN 'IDENTITY' "
    "        ELSE '' "
    "        END AS identity1, "
    "    CASE "
    "        WHEN c.is_nullable = 0 "
    "            THEN 'NOT NULL' "
    "        ELSE '' "
    "        END AS nullable "
    "FROM sys.columns c INNER JOIN sys.objects o "
    "  ON o.object_id = c.object_id "
    "LEFT JOIN sys.types t "
    "  ON t.user_type_id = c.user_type_id "
    "WHERE o.type = 'U' "
    "AND o.name = '{}' "
    "ORDER BY c.column_id ")

# QUERIES FOR FINDING VIEW COLUMNS.

find_view_col_sql = dict()
find_view_col_sql[access] = not_possible_sql
find_view_col_sql[mysql] = not_implemented
find_view_col_sql[oracle] = find_tab_col_sql[oracle]
find_view_col_sql[postgresql] = not_implemented
find_view_col_sql[sqlite] = find_tab_col_sql[sqlite]
find_view_col_sql[sqlserver] = not_implemented

# QUERIES FOR FINDING INDEXES.

find_indexes_sql = dict()
find_indexes_sql[access] = not_possible_sql
find_indexes_sql[mysql] = not_implemented
find_indexes_sql[oracle] = (
    'SELECT index_name, index_type, table_type, '
    "  CASE "
    "    WHEN uniqueness = 'UNIQUE' "
    "      THEN 'Yes' "
    "  ELSE 'No' "
    '  END AS "unique" '
    "FROM user_indexes WHERE table_name = '{}' "
    "ORDER BY index_name")
find_indexes_sql[postgresql] = not_implemented
find_indexes_sql[sqlite] = (
    "SELECT name AS index_name, 'DUMMY' AS index_type, 'DUMMY' AS table_type, "
    "  CASE "
    '    WHEN "unique" = 1 '
    "      THEN 'Yes' "
    "  ELSE 'No' "
    '  END AS "unique", '
    "  CASE "
    "    WHEN partial = 1 THEN 'Yes' "
    "  ELSE 'No' "
    "  END AS partial "
    "FROM pragma_index_list('{}')")
find_indexes_sql[sqlserver] = not_implemented

# QUERIES FOR FINDING INDEX COLUMNS.

find_ind_col_sql = dict()
find_ind_col_sql[access] = not_possible_sql
find_ind_col_sql[mysql] = not_implemented
find_ind_col_sql[oracle] = (
    "SELECT ic.column_position, column_name, descend, "
    "column_expression FROM user_ind_columns ic "
    "LEFT OUTER JOIN user_ind_expressions ie "
    "ON ic.column_position = ie.column_position "
    "AND ic.index_name = ie.index_name "
    "WHERE ic.index_name = '{}' "
    "ORDER BY ic.column_position")
find_ind_col_sql[postgresql] = not_implemented
# TODO NOT DONE YET
find_ind_col_sql[sqlite] = (
    "SELECT seqno AS column_position, name AS column_name, 'ASC', '' "
    "FROM pragma_index_info('{}')")
find_ind_col_sql[sqlserver] = not_implemented
