""" MyQueries.py """

from MyConstants import *

# QUERIES FOR FINDING TABLES.

not_implemented = "FINDING YOUR {} NOT IMPLEMENTED FOR {}."
not_possible_sql = "SQL CANNOT READ THE SCHEMA IN {} THROUGH {}."

find_tables_sql = dict()
find_tables_sql[access] = not_possible_sql
find_tables_sql[mysql] = (
    "SELECT table_name\n"
    "FROM information_schema.tables\n"
    "WHERE table_type = 'BASE TABLE'\n"
    "AND table_schema = database()\n"
    "ORDER BY table_name;")
find_tables_sql[oracle] = (
    "SELECT table_name\n"
    "FROM user_tables\n"
    "ORDER BY table_name")
find_tables_sql[postgresql] = (
    "SELECT table_name\n"
    "FROM information_schema.tables\n"
    "WHERE table_type = 'BASE TABLE'\n"
    "AND table_schema = 'public'\n"
    "ORDER BY table_name;")
find_tables_sql[sqlite] = (
    "SELECT name AS table_name\n"
    "FROM sqlite_master\n"
    "WHERE type='table'\n"
    "AND name NOT LIKE 'sqlite_%'\n"
    "ORDER BY name")
find_tables_sql[sqlserver] = (
    "SELECT name AS table_name\n"
    "FROM sys.tables\n"
    "WHERE type='U'\n"
    "ORDER BY name")

# QUERIES FOR FINDING VIEWS.
# TODO need to use all fields and make return values consistent.
find_views_sql = dict()
find_views_sql[access] = not_possible_sql
find_views_sql[mysql] = (
    "SELECT table_name AS view_name, view_definition AS view_sql,\n"
    "  check_option, is_updatable, 'No' AS is_insertable,\n"
    "  'No' AS is_deletable\n"
    "FROM information_schema.views\n"
    "WHERE table_schema = database()\n"
    "ORDER BY table_name;")
find_views_sql[oracle] = (
    "SELECT view_name, text AS view_sql\n"
    "FROM user_views\n"
    "ORDER BY view_name")
find_views_sql[postgresql] = (
    "SELECT table_name AS view_name, view_definition AS view_sql,\n"
    "  check_option, is_updatable, is_insertable_into AS is_insertable,\n"
    "  'No' AS is_deletable,\n"
    "  is_trigger_insertable_into, is_trigger_updatable, is_trigger_deletable\n"
    "FROM information_schema.views\n"
    "WHERE table_schema ='public'\n"
    "ORDER BY table_name;")
find_views_sql[sqlite] = (
    "SELECT name AS view_name, sql AS view_sql,\n"
    "  'No' AS check_option, 'No' AS is_updatable, 'No' AS is_insertable,\n"
    "  'No' AS is_deletable\n"
    "FROM sqlite_master\n"
    "WHERE type='view'\n"
    "ORDER BY name")
find_views_sql[sqlserver] = (
    "SELECT name AS view_name, object_definition(object_id(name)) AS view_sql,\n"
    "  'No' AS check_option, 'No' AS is_updatable, 'No' AS is_insertable,\n"
    "  'No' AS is_deletable\n"
    "FROM sys.views WHERE type='V'\n"
    "ORDER BY name")

# QUERIES FOR FINDING TABLE COLUMNS.

find_tab_col_sql = dict()
find_tab_col_sql[access] = not_possible_sql
find_tab_col_sql[mysql] = not_implemented
find_tab_col_sql[oracle] = (
    "SELECT column_id, c.column_name,\n"
    "  CASE\n"
    "    WHEN (data_type LIKE '%CHAR%' OR data_type IN ('RAW','UROWID'))\n"
    "      THEN data_type||'('||c.char_length||\n"
    "           DECODE(char_used,'B',' BYTE','C',' CHAR',NULL)||')'\n"
    "    WHEN data_type = 'NUMBER'\n"
    "      THEN\n"
    "        CASE\n"
    "          WHEN c.data_precision IS NULL AND c.data_scale IS NULL\n"
    "            THEN 'NUMBER'\n"
    "          WHEN c.data_precision IS NULL AND c.data_scale IS NOT NULL\n"
    "            THEN 'NUMBER(38,'||c.data_scale||')'\n"
    "          ELSE data_type||'('||c.data_precision||','||c.data_scale||')'\n"
    "          END\n"
    "    WHEN data_type = 'BFILE'\n"
    "      THEN 'BINARY FILE LOB (BFILE)'\n"
    "    WHEN data_type = 'FLOAT'\n"
    "      THEN data_type||'('||to_char(data_precision)||')'||\n"
    "           DECODE(data_precision, 126,' (double precision)',\n"
    "           63,' (real)',NULL)\n"
    "    ELSE data_type\n"
    "    END AS data_type,\n"
    "  DECODE(nullable,'Y','Yes','No') AS nullable,\n"
    "  data_default AS default_value,\n"
    "  NVL(comments,'(null)') AS comments\n"
    "FROM user_tab_cols c, user_col_comments com\n"
    "WHERE c.table_name = '{}'\n"
    "AND c.table_name = com.table_name\n"
    "AND c.column_name = com.column_name\n"
    "ORDER BY column_id")
find_tab_col_sql[postgresql] = not_implemented
find_tab_col_sql[sqlite] = (
    "SELECT cid AS column_id, name AS column_name, type AS data_type,\n"
    "  CASE\n"
    "    WHEN \"notnull\" = 1\n"
    "      THEN 'No'\n"
    "    ELSE 'Yes'\n"
    "    END AS nullable,\n"
    "  CASE\n"
    "    WHEN dflt_value IS NULL\n"
    "      THEN '(null)'\n"
    "    ELSE ''\n"
    "    END AS default_value,\n"
    "  '' AS comments\n"
    "FROM pragma_table_info('{}')\n"
    "ORDER BY column_id")
# z used for both find_tab_col_sql[sqlserver] and find_view_col_sql[sqlserver].
z = (
    "SELECT c.column_id, c.name AS column_name,\n"
    "  CASE\n"
    "    WHEN t.name in ('char','varchar')\n"
    "      THEN CONCAT(t.name,'(',c.max_length,')')\n"
    "    WHEN t.name in ('nchar','nvarchar')\n"
    "      THEN CONCAT(t.name,'(',c.max_length/2,')')\n"
    "    WHEN t.name in ('numeric','decimal')\n"
    "      THEN CONCAT(t.name,'(',c.precision,',',c.scale,')')\n"
    "    WHEN t.name in ('real','float')\n"
    "      THEN CONCAT(t.name,'(',c.precision,')')\n"
    "    WHEN t.name LIKE '%INT'\n"
    "      THEN t.name\n"
    "    WHEN t.name IN ('money','datetime')\n"
    "      THEN t.name\n"
    "    ELSE t.name\n"
    "    END AS data_type,\n"
    "  CASE\n"
    "    WHEN c.is_nullable = 0\n"
    "      THEN 'NOT NULL'\n"
    "    ELSE ''\n"
    "    END AS nullable,\n"
    "  '' AS default_value,\n"
    "  '' AS comments,\n"
    "  CASE\n"
    "    WHEN c.is_identity = 1\n"
    "      THEN 'IDENTITY'\n"
    "    ELSE ''\n"
    "    END AS \"identity\"\n"
    "FROM sys.columns c INNER JOIN sys.objects o\n"
    "  ON o.object_id = c.object_id\n"
    "LEFT JOIN sys.types t\n"
    "  ON t.user_type_id = c.user_type_id\n"
    "WHERE o.type = '{}'\n"
    "AND o.name = '{}'\n"
    "ORDER BY c.column_id ")
find_tab_col_sql[sqlserver] = z.format('U', '{}')
# QUERIES FOR FINDING VIEW COLUMNS.

find_view_col_sql = dict()
find_view_col_sql[access] = not_possible_sql
find_view_col_sql[mysql] = not_implemented
find_view_col_sql[oracle] = find_tab_col_sql[oracle]
find_view_col_sql[postgresql] = not_implemented
find_view_col_sql[sqlite] = find_tab_col_sql[sqlite]
find_view_col_sql[sqlserver] = z.format('V', '{}')

# QUERIES FOR FINDING INDEXES.

find_indexes_sql = dict()
find_indexes_sql[access] = not_possible_sql
find_indexes_sql[mysql] = not_implemented
find_indexes_sql[oracle] = (
    "SELECT index_name, index_type, table_type,\n"
    "  CASE\n"
    "    WHEN uniqueness = 'UNIQUE'\n"
    "      THEN 'Yes'\n"
    "  ELSE 'No'\n"
    "  END AS \"unique\"\n"
    "FROM user_indexes WHERE table_name = '{}'\n"
    "ORDER BY index_name")
find_indexes_sql[postgresql] = not_implemented
find_indexes_sql[sqlite] = (
    "SELECT name AS index_name, '' AS index_type, '' AS table_type,\n"
    "  CASE\n"
    "    WHEN \"unique\" = 1\n"
    "      THEN 'Yes'\n"
    "    ELSE 'No'\n"
    "    END AS \"unique\",\n"
    "  CASE\n"
    "    WHEN partial = 1\n"
    "      THEN 'Yes'\n"
    "    ELSE 'No'\n"
    "    END AS partial\n"
    "FROM pragma_index_list('{}')")
find_indexes_sql[sqlserver] = not_implemented

# QUERIES FOR FINDING INDEX COLUMNS.

find_ind_col_sql = dict()
find_ind_col_sql[access] = not_possible_sql
find_ind_col_sql[mysql] = not_implemented
find_ind_col_sql[oracle] = (
    "SELECT ic.column_position, column_name, descend,\n"
    "  column_expression FROM user_ind_columns ic\n"
    "LEFT OUTER JOIN user_ind_expressions ie\n"
    "ON ic.column_position = ie.column_position\n"
    "AND ic.index_name = ie.index_name\n"
    "WHERE ic.index_name = '{}'\n"
    "ORDER BY ic.column_position")
find_ind_col_sql[postgresql] = not_implemented
find_ind_col_sql[sqlite] = (
    "SELECT seqno AS column_position, name AS column_name,\n"
    "  CASE\n"
    "    WHEN desc = 1\n"
    "      THEN 'DESC'\n"
    "    ELSE 'ASC'\n"
    "    END AS descend,\n"
    "  '' AS column_expression\n"
    "FROM pragma_index_xinfo('{}')\n"
    "WHERE key = 1")
find_ind_col_sql[sqlserver] = not_implemented
