{% test valid_temperature(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < -50 OR {{ column_name }} > 60
{% endtest %}