def parse_expression(expr: str):
    """Very basic Talend-to-PySpark expression parser."""
    expr = expr.replace("row1.", "col_")
    expr = expr.replace("+", " + ").replace("-", " - ")
    return expr
