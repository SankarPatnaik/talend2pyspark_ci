from src.talend2pyspark.expression_parser import parse_expression

def test_basic_expr():
    expr = "row1.col1+row1.col2"
    result = parse_expression(expr)
    assert "col_col1" in result
    assert "+" in result
