CREATE OR REPLACE PROCEDURE "LOAD_TABLE_DYNAMIC"("V_CONFIG_NAME" VARCHAR, "V_CALC_DT" DATE, "V_CREATE_TABLE" BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','jinja2')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
from jinja2 import Template, StrictUndefined
import json
from collections import defaultdict

def get_etl_variables(session, config_name, calc_dt, load_level, level_value):

    # Get ETL load variable info
    
    template = Template("""
        SELECT VARIABLE_TABLE, VARIABLE_COLUMN, VARIABLE_NAME
        FROM CONFIG_ALL.ETL_LOAD_VARIABLES
        WHERE CONFIG_NAME = ''{{ config_name }}''
    """)
    rendered_sql = template.render(config_name = config_name)
    rows = session.sql(rendered_sql).collect()

    # Get ETL load variable values

    variables = {}

    if rows:
        context = {
            "var_table": rows[0][''VARIABLE_TABLE''],
            "var_column": rows[0][''VARIABLE_COLUMN''],
            "var_name": rows[0][''VARIABLE_NAME''],
            "load_level": load_level,
            "level_value": level_value,
            "calc_dt": calc_dt
        }
    
        template = Template("""
            SELECT 
                f.VALUE
            FROM 
                {{ var_table }},
                LATERAL FLATTEN(input => PARSE_JSON({{ var_column }})) f
            WHERE 
                {{ var_table }}.VARIABLE_NAME = ''{{ var_name }}''
            AND CALC_DT = DATE''{{ calc_dt }}''
            {%- if load_level %}
            AND {{ load_level }} = {{ level_value }}
            {%- endif %};
        """)
        rendered_sql = template.render(**context)
        rows = session.sql(rendered_sql).collect()
    
        # Load json result to dict with lowercased var names
        variables = json.loads(rows[0][''VALUE''])
    
    return variables

def main(session,v_config_name,v_calc_dt,v_create_table):

    ''''''
    STEP 1: Load/configure JSON config and initialize variables
    ''''''
    config_sql = """
    SELECT LOAD_TYPE, LOAD_LEVEL, LEVEL_VALUE, CONFIG_JSON
        FROM CONFIG_ALL.ETL_LOAD_CONFIG
        WHERE CONFIG_NAME = :1
    """
    config_df = session.sql(config_sql,params=[v_config_name])
    config_rows = config_df.collect()

    load_type = config_rows[0][''LOAD_TYPE'']
    load_level = config_rows[0][''LOAD_LEVEL'']
    level_value = config_rows[0][''LEVEL_VALUE'']
    config_str = config_rows[0][''CONFIG_JSON'']

    # Add variables to context
    context = get_etl_variables(session, v_config_name, v_calc_dt, load_level, level_value)
    context["V_CALC_DT"] = v_calc_dt
    if load_level: 
        context[load_level] = level_value

    template = Template(config_str, undefined=StrictUndefined) # StrictUndefined warns about any undefined variables
    rendered = template.render(**context)
    config = json.loads(rendered)

    exposed_columns = []
    json_groups = defaultdict(list)
    table_columns = {}

    ''''''
    STEP 2: Extract EXPOSED fields vs JSON fields from config columns
    ''''''
    for col in config["columns"]:
        expression = col["transformation"] or col["source"]
        target_type = col["target_type"].upper()

        if target_type == "EXPOSED":
            exposed_columns.append({
                "name": col["target"],
                "expression": expression
            })
            
            if v_create_table: table_columns[f''{col["target"]} {col["data_type"]}''] = col["ordinal_position"]
        else:
            json_groups[target_type].append({
                "name": col["target"],
                "expression": expression
            })
            if v_create_table: table_columns[f''"{target_type}" VARIANT''] = 9999  

    ''''''
    STEP 3: Create target table if v_create_table flag set to TRUE
    ''''''
    if v_create_table:

        # IF THIS FAILS, AN ORDINAL POSITION SOMEWHERE IS BLANK

        table_columns["RUN_DTTM TIMESTAMP"] = max(table_columns.values(), default=0) + 1
        
        ordered_column_names = [k for k, v in sorted(table_columns.items(), key=lambda item: item[1])]

        # Define Jinja template for CREATE TABLE SQL
        template = Template("""
        CREATE OR REPLACE TABLE {{ target_table }} (
            {{ ordered_column_names | join('',\\n    '') }}
        );
        """)
        
        rendered_sql = template.render(**config,ordered_column_names=ordered_column_names)
        
        # Execute generated SQL
        session.sql(rendered_sql).collect()

    ''''''
    STEP 4: Delete from target table if load type DELETE
    ''''''
    if load_type == "DELETE" or load_type == "INSERT_DELETE":
        # Define Jinja template for DELETE SQL 
        template = Template("""
        DELETE FROM {{ target_table }}
        {%- if delete_filter %}
        WHERE {{ delete_filter }}
        {%- endif %}
        """)
    
        rendered_sql = template.render(**config)

        # Execute template SQL
        session.sql(rendered_sql).collect()

    ''''''
    STEP 5: Insert into target table if load type INSERT
    '''''' 
    if load_type == "INSERT" or load_type == "INSERT_DELETE":
        template = Template("""
        INSERT INTO {{ target_table }}
        (
            {%- for col in exposed_columns %}
                {{ col.name }},
            {%- endfor %}
            {%- for json_col in json_groups.keys() %}
                {{ json_col }},
            {%- endfor %}
                RUN_DTTM
        )
        SELECT
            {%- for col in exposed_columns %}
                {{ col.expression }} AS {{ col.name }},
            {%- endfor %}
            {%- for json_col, fields in json_groups.items() %}
                OBJECT_CONSTRUCT_KEEP_NULL(
                    {%- for field in fields %}
                        ''{{ field.name }}'', {{ field.expression }}{{ "," if not loop.last }}
                    {%- endfor %}
                ) AS {{ json_col }},
            {%- endfor %}
                CURRENT_TIMESTAMP() AS RUN_DTTM
        FROM {{ source_table }} {{ source_alias }}
        {% for join in joins -%}
        {{ join.type }} JOIN {{ join.table }} {{ join.alias }} ON {{ join.on }}
        {% endfor -%}
        {%- if load_filter %}
        WHERE {{ load_filter }}
        {%- endif %}
        {%- if group_by %}
        GROUP BY {{ group_by }}
        {%- endif %}
        """)
        
        rendered_sql = template.render(**config, exposed_columns=exposed_columns, json_groups=json_groups)
        
        # Execute template SQL
        result = session.sql(rendered_sql).collect()

        # Store how many rows were inserted
        rows_inserted = len(result)

        ''''''
        STEP 6: Output results
        ''''''
    
        return f"Successfully loaded table"
    return "SUCCESS"
';