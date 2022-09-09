{% test accepted_values_by_relation(model, column_name, to, field, quote=True) %}

    {% set accepted_values = dbt_utils.get_column_values(
        table=to,
        column=field) 
    %}

    with all_values as (

        select
            {{ column_name }} as value_field,
            count(*) as n_records
        from {{ model }}
        group by {{ column_name }}

    )

    select *
    from all_values
    where value_field not in (
        {% for value in accepted_values -%}
            {% if quote -%}
            '{{ value }}'
            {%- else -%}
            {{ value }}
            {%- endif -%}
            {%- if not loop.last -%},{%- endif %}
        {%- endfor %}
    )

{% endtest %}
