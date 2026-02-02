{% macro calculate_conversion_rate(numerator, denominator) %}
    {#-
        Safely calculate conversion rate avoiding division by zero.

        Args:
            numerator: The count of converted items
            denominator: The total count

        Returns:
            Conversion rate as a float between 0 and 1
    -#}
    CASE
        WHEN {{ denominator }} > 0
        THEN CAST({{ numerator }} AS FLOAT) / {{ denominator }}
        ELSE 0
    END
{% endmacro %}


{% macro calculate_growth_rate(current_value, previous_value) %}
    {#-
        Calculate period-over-period growth rate.

        Args:
            current_value: The current period's value
            previous_value: The previous period's value

        Returns:
            Growth rate as a decimal (0.1 = 10% growth)
    -#}
    CASE
        WHEN {{ previous_value }} > 0
        THEN ({{ current_value }} - {{ previous_value }}) / CAST({{ previous_value }} AS FLOAT)
        ELSE 0
    END
{% endmacro %}


{% macro safe_divide(numerator, denominator, default_value=0) %}
    {#-
        Safe division that returns a default value when denominator is zero.

        Args:
            numerator: The numerator
            denominator: The denominator
            default_value: Value to return if denominator is 0 (default: 0)

        Returns:
            Division result or default value
    -#}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
        THEN {{ default_value }}
        ELSE {{ numerator }} / CAST({{ denominator }} AS FLOAT)
    END
{% endmacro %}


{% macro percentile_bucket(value, percentile_column) %}
    {#-
        Assign a value to a percentile bucket.

        Args:
            value: The value to bucket
            percentile_column: Column name to calculate percentiles from

        Returns:
            Bucket number (1-5)
    -#}
    NTILE(5) OVER (ORDER BY {{ value }})
{% endmacro %}


{% macro event_count_by_type(event_type) %}
    {#-
        Generate a count expression for a specific event type.

        Args:
            event_type: The event type to count

        Returns:
            SQL CASE expression for counting
    -#}
    SUM(CASE WHEN event_type = '{{ event_type }}' THEN 1 ELSE 0 END)
{% endmacro %}


{% macro revenue_by_condition(condition) %}
    {#-
        Sum revenue where a condition is met.

        Args:
            condition: SQL condition to filter revenue

        Returns:
            SQL expression for conditional revenue sum
    -#}
    SUM(CASE WHEN {{ condition }} THEN revenue_amount ELSE 0 END)
{% endmacro %}
