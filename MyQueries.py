""" MyQueries.py """
from MyConstants import *

not_implemented = "FINDING YOUR {} NOT IMPLEMENTED FOR {}."
not_possible_sql = "SQL CANNOT READ THE SCHEMA IN {} THROUGH {}."

tables = "TABLES"
tab_col = "TABLE COLUMNS"
views = "VIEWS"
view_col = "VIEW COLUMNS"
indexes = "INDEXES"
ind_col = "INDEX COLUMNS"

data_dict_sql = dict()

# QUERIES FOR FINDING TABLES.

data_dict_sql[tables, access] = not_possible_sql
data_dict_sql[tables, mysql] = (
    "SELECT table_name\n"
    "FROM information_schema.tables\n"
    "WHERE table_type = 'BASE TABLE'\n"
    "AND table_schema = database()\n"
    "ORDER BY table_name;")
data_dict_sql[tables, oracle] = (
    "SELECT table_name\n"
    "FROM user_tables\n"
    "ORDER BY table_name")
data_dict_sql[tables, postgresql] = (
    "SELECT table_name\n"
    "FROM information_schema.tables\n"
    "WHERE table_type = 'BASE TABLE'\n"
    "AND table_schema = 'public'\n"
    "ORDER BY table_name;")
data_dict_sql[tables, sqlite] = (
    "SELECT name AS table_name\n"
    "FROM sqlite_master\n"
    "WHERE type='table'\n"
    "AND name NOT LIKE 'sqlite_%'\n"
    "ORDER BY name")
data_dict_sql[tables, sqlserver] = (
    "SELECT name AS table_name\n"
    "FROM sys.tables\n"
    "WHERE type='U'\n"
    "ORDER BY name")

# QUERIES FOR FINDING VIEWS.

# TODO need to use all fields and make return values consistent.
data_dict_sql[views, access] = not_possible_sql
data_dict_sql[views, mysql] = (
    "SELECT table_name AS view_name, view_definition AS view_sql,\n"
    "  check_option, is_updatable, 'No' AS is_insertable,\n"
    "  'No' AS is_deletable\n"
    "FROM information_schema.views\n"
    "WHERE table_schema = database()\n"
    "ORDER BY table_name;")
data_dict_sql[views, oracle] = (
    "SELECT view_name, text AS view_sql\n"
    "FROM user_views\n"
    "ORDER BY view_name")
data_dict_sql[views, postgresql] = (
    "SELECT table_name AS view_name, view_definition AS view_sql,\n"
    "  check_option, is_updatable, is_insertable_into AS is_insertable,\n"
    "  'No' AS is_deletable,\n"
    "  is_trigger_insertable_into, is_trigger_updatable, is_trigger_deletable\n"
    "FROM information_schema.views\n"
    "WHERE table_schema = 'public'\n"
    "ORDER BY table_name;")
data_dict_sql[views, sqlite] = (
    "SELECT name AS view_name, sql AS view_sql,\n"
    "  'No' AS check_option, 'No' AS is_updatable, 'No' AS is_insertable,\n"
    "  'No' AS is_deletable\n"
    "FROM sqlite_master\n"
    "WHERE type='view'\n"
    "ORDER BY name")
data_dict_sql[views, sqlserver] = (
    "SELECT name AS view_name, object_definition(object_id(name)) AS "
    "  view_sql,\n"
    "  'No' AS check_option, 'No' AS is_updatable, 'No' AS is_insertable,\n"
    "  'No' AS is_deletable\n"
    "FROM sys.views WHERE type='V'\n"
    "ORDER BY name")

# QUERIES FOR FINDING TABLE COLUMNS.

data_dict_sql[tab_col, access] = not_possible_sql
data_dict_sql[tab_col, mysql] = (
    "SELECT ordinal_position AS column_id, column_name,\n"
    "  column_type AS data_type, is_nullable as nullable,\n"
    "  column_default AS default_value, column_comment AS comments\n"
    "FROM INFORMATION_SCHEMA.COLUMNS\n"
    "WHERE table_name = '{}'\n"
    "AND table_schema = database()")
data_dict_sql[tab_col, oracle] = (
    "SELECT column_id, c.column_name,\n"
    "  CASE\n"
    "    WHEN (data_type LIKE '%CHAR%' OR data_type IN ('RAW','UROWID'))\n"
    "      THEN data_type||'('||c.char_length||"
    "          DECODE(char_used,'B',' BYTE','C',' CHAR','')||')'\n"
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
    "      THEN data_type||'('||to_char(data_precision)||')'||DECODE("
    "          data_precision, 126,' (double precision)', 63,' (real)','')\n"
    "    ELSE data_type\n"
    "    END AS data_type,\n"
    "  DECODE(nullable,'Y','Yes','No') AS nullable,\n"
    "  data_default AS default_value,\n"
    "  comments\n"
    "FROM user_tab_cols c, user_col_comments com\n"
    "WHERE c.table_name = '{}'\n"
    "AND c.table_name = com.table_name\n"
    "AND c.column_name = com.column_name\n"
    "ORDER BY column_id")
data_dict_sql[tab_col, postgresql] = (
    "SELECT ordinal_position AS column_id, column_name,\n"
    "  CASE \n"
    "    WHEN data_type = 'character varying'\n"
    "      THEN 'varchar('||character_maximum_length||')'\n"
    "    WHEN data_type = 'bit'\n"
    "      THEN 'bit('||character_maximum_length||')'\n"
    "    WHEN data_type = 'bit varying'\n"
    "      THEN 'varbit('||character_maximum_length||')'\n"
    "    WHEN data_type = 'character'\n"
    "      THEN 'char('||character_maximum_length||')'\n"
    "    WHEN data_type='numeric' AND numeric_precision IS NOT NULL AND"
    "          numeric_scale IS NOT NULL\n"
    "      THEN 'numeric('||numeric_precision||','||numeric_scale||')'\n"
    "    WHEN data_type IN ('bigint', 'boolean', 'date', 'double precision',"
    "          'integer', 'money', 'numeric', 'real', 'smallint', 'text')\n"
    "      THEN data_type\n"
    "    WHEN data_type LIKE 'timestamp%' AND datetime_precision != 6\n"
    "      THEN REPLACE(data_type, 'timestamp', "
    "          'timestamp('||datetime_precision||')')\n"
    "    WHEN data_type LIKE 'time%' AND datetime_precision != 6\n"
    "      THEN REGEXP_REPLACE(data_type, '^time',"
    "          'time('||datetime_precision||')')\n"
    "    ELSE data_type\n"
    "    END AS data_type,\n"
    "  is_nullable AS nullable,\n"
    "  column_default AS default_value,\n"
    "  '' AS comments\n"
    "FROM INFORMATION_SCHEMA.COLUMNS\n"
    "WHERE table_name = '{}'\n"
    "AND table_schema = 'public'")
data_dict_sql[tab_col, sqlite] = (
    "SELECT cid AS column_id, name AS column_name, type AS data_type,\n"
    "  CASE\n"
    "    WHEN \"notnull\" = 1\n"
    "      THEN 'No'\n"
    "    ELSE 'Yes'\n"
    "    END AS nullable,\n"
    "  dflt_value AS default_value,\n"
    "  '' AS comments\n"
    "FROM pragma_table_info('{}')\n"
    "ORDER BY column_id")
# Used for both data_dict_sql[tab_col, sqlserver]
#   and data_dict_sql[view_col, sqlserver].
tab_col_sqlserver = (
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
data_dict_sql[tab_col, sqlserver] = tab_col_sqlserver.format('U', '{}')

# QUERIES FOR FINDING VIEW COLUMNS.

data_dict_sql[view_col, access] = not_possible_sql
# TODO CHECK THIS
data_dict_sql[view_col, mysql] = data_dict_sql[tab_col, mysql]
data_dict_sql[view_col, oracle] = data_dict_sql[tab_col, oracle]
# TODO CHECK THIS
data_dict_sql[view_col, postgresql] = data_dict_sql[tab_col, postgresql]
data_dict_sql[view_col, sqlite] = data_dict_sql[tab_col, sqlite]
data_dict_sql[view_col, sqlserver] = tab_col_sqlserver.format('V', '{}')

# QUERIES FOR FINDING INDEXES.

data_dict_sql[indexes, access] = not_possible_sql
data_dict_sql[indexes, mysql] = not_implemented
data_dict_sql[indexes, oracle] = (
    "SELECT index_name, index_type, table_type,\n"
    "  CASE\n"
    "    WHEN uniqueness = 'UNIQUE'\n"
    "      THEN 'Yes'\n"
    "  ELSE 'No'\n"
    "  END AS \"unique\"\n"
    "FROM user_indexes WHERE table_name = '{}'\n"
    "ORDER BY index_name")
data_dict_sql[indexes, postgresql] = not_implemented
data_dict_sql[indexes, sqlite] = (
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
data_dict_sql[indexes, sqlserver] = not_implemented

# QUERIES FOR FINDING INDEX COLUMNS.

data_dict_sql[ind_col, access] = not_possible_sql
data_dict_sql[ind_col, mysql] = not_implemented
data_dict_sql[ind_col, oracle] = (
    "SELECT ic.column_position, column_name, descend,\n"
    "  column_expression FROM user_ind_columns ic\n"
    "LEFT OUTER JOIN user_ind_expressions ie\n"
    "ON ic.column_position = ie.column_position\n"
    "AND ic.index_name = ie.index_name\n"
    "WHERE ic.index_name = '{}'\n"
    "ORDER BY ic.column_position")
data_dict_sql[ind_col, postgresql] = not_implemented
data_dict_sql[ind_col, sqlite] = (
    "SELECT seqno AS column_position, name AS column_name,\n"
    "  CASE\n"
    "    WHEN desc = 1\n"
    "      THEN 'DESC'\n"
    "    ELSE 'ASC'\n"
    "    END AS descend,\n"
    "  '' AS column_expression\n"
    "FROM pragma_index_xinfo('{}')\n"
    "WHERE key = 1")
data_dict_sql[ind_col, sqlserver] = not_implemented
