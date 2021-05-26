{% materialization idempotent_incremental, adapter='athena' -%}

  {% set unique_key = config.get('unique_key') %}

  {% set overwrite_msg -%}
    Athena adapter does not support 'unique_key'
  {%- endset %}
  {% if unique_key is not none %}
    {% do exceptions.raise_compiler_error(overwrite_msg) %}
  {% endif %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% set existing_s3_urls = [] %}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or should_full_refresh() %}
      {% do adapter.drop_relation(existing_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
      {% set overwrite_partitions_where = config.require('overwrite_partitions_where') %}
      {% set existing_s3_urls = adapter.s3_list_objects_in_partitions(existing_relation, overwrite_partitions_where) %}

      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% set build_sql = incremental_insert(tmp_relation, target_relation) %}
      {% do to_drop.append(tmp_relation) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  -- set table properties
  {% if not to_drop %}
    {{ set_table_classification(target_relation, 'parquet') }}
  {% endif %}

  {% do adapter.s3_delete_objects(existing_s3_urls) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
