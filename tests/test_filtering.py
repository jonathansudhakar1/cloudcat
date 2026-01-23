"""Tests for WHERE clause parsing and filtering."""

import pytest
import pandas as pd
from cloudcat.filtering import parse_where_clause, apply_where_filter


class TestParseWhereClause:
    """Tests for parse_where_clause function."""

    def test_equals_operator(self):
        col, op, val = parse_where_clause("status=active")
        assert col == "status"
        assert op == "="
        assert val == "active"

    def test_not_equals_operator(self):
        col, op, val = parse_where_clause("status!=deleted")
        assert col == "status"
        assert op == "!="
        assert val == "deleted"

    def test_greater_than_operator(self):
        col, op, val = parse_where_clause("age>30")
        assert col == "age"
        assert op == ">"
        assert val == "30"

    def test_less_than_operator(self):
        col, op, val = parse_where_clause("price<100")
        assert col == "price"
        assert op == "<"
        assert val == "100"

    def test_greater_than_or_equal_operator(self):
        col, op, val = parse_where_clause("count>=10")
        assert col == "count"
        assert op == ">="
        assert val == "10"

    def test_less_than_or_equal_operator(self):
        col, op, val = parse_where_clause("score<=50")
        assert col == "score"
        assert op == "<="
        assert val == "50"

    def test_contains_operator(self):
        col, op, val = parse_where_clause("name contains john")
        assert col == "name"
        assert op == "contains"
        assert val == "john"

    def test_not_contains_operator(self):
        col, op, val = parse_where_clause("message not contains error")
        assert col == "message"
        assert op == "not contains"
        assert val == "error"

    def test_startswith_operator(self):
        col, op, val = parse_where_clause("email startswith admin")
        assert col == "email"
        assert op == "startswith"
        assert val == "admin"

    def test_endswith_operator(self):
        col, op, val = parse_where_clause("file endswith .csv")
        assert col == "file"
        assert op == "endswith"
        assert val == ".csv"

    def test_quoted_value_double_quotes(self):
        col, op, val = parse_where_clause('name="John Doe"')
        assert col == "name"
        assert op == "="
        assert val == "John Doe"

    def test_quoted_value_single_quotes(self):
        col, op, val = parse_where_clause("name='Jane Doe'")
        assert col == "name"
        assert op == "="
        assert val == "Jane Doe"

    def test_value_with_spaces_in_contains(self):
        col, op, val = parse_where_clause("description contains hello world")
        assert col == "description"
        assert op == "contains"
        assert val == "hello world"

    def test_invalid_where_clause_raises_error(self):
        with pytest.raises(ValueError, match="Invalid WHERE clause"):
            parse_where_clause("invalid clause without operator")

    def test_column_with_spaces_around_operator(self):
        col, op, val = parse_where_clause("status = active")
        assert col == "status"
        assert op == "="
        assert val == "active"


class TestApplyWhereFilter:
    """Tests for apply_where_filter function."""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            "name": ["John", "Jane", "Bob", "Alice"],
            "age": [25, 30, 35, 28],
            "city": ["NYC", "LA", "SF", "NYC"],
            "status": ["active", "inactive", "active", "active"]
        })

    def test_equals_filter(self, sample_df):
        result = apply_where_filter(sample_df, "status=active")
        assert len(result) == 3
        assert all(result["status"] == "active")

    def test_not_equals_filter(self, sample_df):
        result = apply_where_filter(sample_df, "status!=active")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Jane"

    def test_greater_than_filter(self, sample_df):
        result = apply_where_filter(sample_df, "age>28")
        assert len(result) == 2
        assert set(result["name"]) == {"Jane", "Bob"}

    def test_less_than_filter(self, sample_df):
        result = apply_where_filter(sample_df, "age<30")
        assert len(result) == 2
        assert set(result["name"]) == {"John", "Alice"}

    def test_greater_than_or_equal_filter(self, sample_df):
        result = apply_where_filter(sample_df, "age>=30")
        assert len(result) == 2
        assert set(result["name"]) == {"Jane", "Bob"}

    def test_less_than_or_equal_filter(self, sample_df):
        result = apply_where_filter(sample_df, "age<=28")
        assert len(result) == 2
        assert set(result["name"]) == {"John", "Alice"}

    def test_contains_filter(self, sample_df):
        result = apply_where_filter(sample_df, "name contains o")
        assert len(result) == 2
        assert set(result["name"]) == {"John", "Bob"}

    def test_contains_filter_case_insensitive(self, sample_df):
        result = apply_where_filter(sample_df, "name contains J")
        assert len(result) == 2
        assert set(result["name"]) == {"John", "Jane"}

    def test_not_contains_filter(self, sample_df):
        result = apply_where_filter(sample_df, "name not contains o")
        assert len(result) == 2
        assert set(result["name"]) == {"Jane", "Alice"}

    def test_startswith_filter(self, sample_df):
        result = apply_where_filter(sample_df, "name startswith J")
        assert len(result) == 2
        assert set(result["name"]) == {"John", "Jane"}

    def test_endswith_filter(self, sample_df):
        result = apply_where_filter(sample_df, "name endswith e")
        assert len(result) == 2
        assert set(result["name"]) == {"Jane", "Alice"}

    def test_empty_dataframe(self, sample_df):
        empty_df = pd.DataFrame()
        result = apply_where_filter(empty_df, "status=active")
        assert result.empty

    def test_none_where_clause(self, sample_df):
        result = apply_where_filter(sample_df, None)
        assert len(result) == len(sample_df)

    def test_empty_where_clause(self, sample_df):
        result = apply_where_filter(sample_df, "")
        assert len(result) == len(sample_df)

    def test_column_not_found_raises_error(self, sample_df):
        with pytest.raises(ValueError, match="Column 'invalid' not found"):
            apply_where_filter(sample_df, "invalid=value")

    def test_numeric_comparison_with_string_value(self, sample_df):
        # Should convert string "30" to int for comparison
        result = apply_where_filter(sample_df, "age=30")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Jane"

    def test_float_comparison(self):
        df = pd.DataFrame({"price": [10.5, 20.0, 30.5, 40.0]})
        result = apply_where_filter(df, "price>25.0")
        assert len(result) == 2

    def test_boolean_column(self):
        df = pd.DataFrame({
            "name": ["John", "Jane"],
            "active": [True, False]
        })
        result = apply_where_filter(df, "active=true")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "John"

    def test_filter_preserves_index_reset(self, sample_df):
        result = apply_where_filter(sample_df, "city=NYC")
        # Index should be preserved (original indices)
        assert list(result.index) == [0, 3]

    def test_startswith_case_insensitive(self, sample_df):
        # Should match "John" and "Jane" even with uppercase J in filter
        result = apply_where_filter(sample_df, "name startswith j")
        assert len(result) == 2
        assert set(result["name"]) == {"John", "Jane"}

    def test_endswith_case_insensitive(self, sample_df):
        # Should match "Jane" and "Alice" even with uppercase E in filter
        result = apply_where_filter(sample_df, "name endswith E")
        assert len(result) == 2
        assert set(result["name"]) == {"Jane", "Alice"}


class TestWhereClauseUppercaseOperators:
    """Tests for WHERE clause with uppercase operators."""

    def test_uppercase_contains_operator(self):
        df = pd.DataFrame({"name": ["John", "Jane"]})
        result = apply_where_filter(df, "name CONTAINS j")
        assert len(result) == 2

    def test_uppercase_startswith_operator(self):
        df = pd.DataFrame({"name": ["John", "Jane", "Bob"]})
        result = apply_where_filter(df, "name STARTSWITH j")
        assert len(result) == 2
        assert set(result["name"]) == {"John", "Jane"}

    def test_uppercase_not_contains_operator(self):
        df = pd.DataFrame({"name": ["John", "Jane", "Bob"]})
        result = apply_where_filter(df, "name NOT CONTAINS o")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Jane"
