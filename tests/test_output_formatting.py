import pytest
import pandas as pd
import json
from cloudcat.cli import format_table_with_colored_header, colorize_json


class TestOutputFormatting:
    def test_format_table_with_colored_header_basic(self):
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "age": [25, 30],
            "city": ["NYC", "LA"]
        })
        
        result = format_table_with_colored_header(df)
        
        assert "John" in result
        assert "Jane" in result
        assert "25" in result
        assert "30" in result

    def test_format_table_with_colored_header_empty_df(self):
        df = pd.DataFrame()
        
        result = format_table_with_colored_header(df)
        
        assert result == "Empty dataset"

    def test_format_table_with_single_row(self):
        df = pd.DataFrame({
            "name": ["John"],
            "age": [25]
        })
        
        result = format_table_with_colored_header(df)
        
        assert "John" in result
        assert "25" in result

    def test_colorize_json_basic(self):
        data = [{"name": "John", "age": 25, "active": True}]
        json_str = json.dumps(data)
        
        result = colorize_json(json_str)
        
        assert "John" in result
        assert "25" in result
        assert "true" in result

    def test_colorize_json_with_null_values(self):
        data = [{"name": "John", "score": None, "active": False}]
        json_str = json.dumps(data)
        
        result = colorize_json(json_str)
        
        assert "John" in result
        assert "null" in result
        assert "false" in result

    def test_colorize_json_with_nested_objects(self):
        data = [{"user": {"name": "John", "age": 25}, "active": True}]
        json_str = json.dumps(data)
        
        result = colorize_json(json_str)
        
        assert "user" in result
        assert "John" in result

    def test_colorize_json_with_numbers(self):
        data = [{"int_val": 42, "float_val": 3.14, "zero": 0}]
        json_str = json.dumps(data)
        
        result = colorize_json(json_str)
        
        assert "42" in result
        assert "3.14" in result
        assert "0" in result

    def test_format_table_with_special_characters(self):
        df = pd.DataFrame({
            "name": ["John & Jane", "Bob's Data"],
            "value": ["<test>", "data@example.com"]
        })
        
        result = format_table_with_colored_header(df)
        
        assert "John & Jane" in result
        assert "Bob's Data" in result
        assert "<test>" in result
        assert "data@example.com" in result

    def test_format_table_with_numeric_data(self):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "score": [95.5, 87.2, 92.8],
            "count": [100, 200, 150]
        })
        
        result = format_table_with_colored_header(df)
        
        assert "95.5" in result
        assert "87.2" in result
        assert "100" in result
        assert "200" in result