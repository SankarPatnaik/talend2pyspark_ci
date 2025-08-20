from .expression_parser import parse_expression

def convert_metadata_to_plan(metadata):
    # Placeholder for demo
    return [
        {"type": "tMap", "output": "df.withColumn('new_col', expr('col_a + col_b'))"},
        {"type": "tJoin", "output": "df1.join(df2, df1.id == df2.id, 'inner')"},
        {"type": "tAggregateRow", "output": "df.groupBy('category').agg({'amount':'sum'})"}
    ]
