import re

# Expression translation helpers
def translate_expression(expr, df_alias="df"):
    """
    Translates a Talend expression into a PySpark expression string.
    This is a simplified parser (can be extended).
    """

    # Map Talend functions → PySpark functions
    replacements = {
        r"(?i)\.toUpperCase\(\)": "F.upper",
        r"(?i)\.toLowerCase\(\)": "F.lower",
        r"StringHandling\.TRIM\((.*?)\)": r"F.trim(\1)",
        r"StringHandling\.LEFT\((.*?),\s*(\d+)\)": r"F.substring(\1, 1, \2)",
        r"StringHandling\.RIGHT\((.*?),\s*(\d+)\)": r"F.expr(f'substring(\1, length(\1)-\2+1, \2)')",
        r"StringHandling\.CONCAT\((.*?),(.*?)\)": r"F.concat(\1, \2)"
    }

    # Replace ternary operators (a ? b : c)
    if "?" in expr and ":" in expr:
        m = re.match(r"\((.*?)\)\s*\?\s*(.*?)\s*:\s*(.*)", expr)
        if m:
            condition, true_val, false_val = m.groups()
            return f"F.when({translate_expression(condition.strip(), df_alias)}, {translate_expression(true_val.strip(), df_alias)}).otherwise({translate_expression(false_val.strip(), df_alias)})"

    # Replace row.column → df["column"]
    expr = re.sub(r"row\.([a-zA-Z0-9_]+)", rf'{df_alias}["\1"]', expr)

    # Apply function replacements
    for pattern, repl in replacements.items():
        expr = re.sub(pattern, repl, expr)

    # Wrap string literals with F.lit if needed
    expr = re.sub(r'"([^"]*)"', r'F.lit("\1")', expr)

    return expr
