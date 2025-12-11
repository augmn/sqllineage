from sqlfluff.core.parser import BaseSegment

from sqllineage import SQLPARSE_DIALECT
from sqllineage.core.models import Column, Schema, SubQuery, Table
from sqllineage.core.parser.sqlfluff.utils import (
    extract_column_qualifier,
    extract_identifier,
    is_subquery,
    is_teradata_title_phrase,
    is_wildcard,
    list_child_segments,
)
from sqllineage.utils.entities import ColumnQualifierTuple
from sqllineage.utils.helpers import escape_identifier_name

NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE = [
    "partitionby_clause",
    "orderby_clause",
    "expression",
    "case_expression",
    "when_clause",
    "else_clause",
    "select_clause_element",
    "cast_expression",
]

FUNCTION_SEGMENT_TYPE = ["function"]

COLUMN_SEGMENT_TYPE = ["identifier", "column_reference"]

SOURCE_COLUMN_SEGMENT_TYPE = (
    NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE + FUNCTION_SEGMENT_TYPE + COLUMN_SEGMENT_TYPE
)


class SqlFluffTable(Table):
    """
    Data Class for SqlFluffTable
    """

    @staticmethod
    def of(table: BaseSegment, alias: str | None = None) -> Table:
        """
        Build an object of type 'Table'
        :param table: table segment to be processed
        :param alias: alias of the table segment
        :return: 'Table' object
        """
        dot_idx = None
        for idx in range(len(table.segments) - 2, -1, -1):
            token = table.segments[idx]
            if bool(token.type == "symbol"):
                dot_idx, _ = idx, token
                break
        real_name = (
            table.segments[dot_idx + 1].raw
            if dot_idx
            else (table.raw if table.type == "identifier" else table.segments[0].raw)
        )
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(segment.raw)
                    for segment in table.segments[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = Schema(parent_name) if parent_name is not None else Schema()
        kwargs = {"alias": alias} if alias else {}
        return Table(real_name, schema, **kwargs)


class SqlFluffSubQuery(SubQuery):
    """
    Data Class for SqlFluffSubQuery
    """

    @staticmethod
    def of(subquery: BaseSegment, alias: str | None) -> SubQuery:
        """
        Build a 'SubQuery' object
        :param subquery: subquery segment
        :param alias: subquery alias
        :return: 'SubQuery' object
        """
        return SubQuery(subquery, subquery.raw, alias)


class SqlFluffColumn(Column):
    """
    Data Class for SqlFluffColumn
    """

    @staticmethod
    def of(column: BaseSegment, **kwargs) -> Column:
        """
        Build a 'Column' object
        :param column: column segment
        :return: 'Column' object
        """
        if column.type == "select_clause_element":

            # Special handling for Teradata TITLE phrase
            if is_teradata_title_phrase(column):
                function_name_identifier = next(
                    column.recursive_crawl("function_name_identifier")
                )
                return Column(
                    function_name_identifier.raw,
                    source_columns=[
                        ColumnQualifierTuple(function_name_identifier.raw, None)
                    ],
                )

            source_columns, alias = SqlFluffColumn._get_column_and_alias(column)
            if alias:
                return Column(alias, source_columns=source_columns, from_alias=True)
            if source_columns:
                column_name = None
                for sub_segment in list_child_segments(column):
                    if sub_segment.type == "column_reference" or is_wildcard(
                        sub_segment
                    ):
                        if cqt := extract_column_qualifier(sub_segment):
                            column_name = cqt.column
                    elif sub_segment.type == "expression":
                        # special handling for postgres style type cast, col as target column name instead of col::type
                        if len(sub2_segments := list_child_segments(sub_segment)) == 1:
                            if (
                                sub2_segment := sub2_segments[0]
                            ).type == "cast_expression":
                                if (
                                    len(
                                        sub3_segments := list_child_segments(
                                            sub2_segment
                                        )
                                    )
                                    == 2
                                ):
                                    if (
                                        sub3_segment := sub3_segments[0]
                                    ).type == "column_reference":
                                        if cqt := extract_column_qualifier(
                                            sub3_segment
                                        ):
                                            column_name = cqt.column
                return Column(
                    column.raw if column_name is None else column_name,
                    source_columns=source_columns,
                )

        # Wildcard, Case, Function without alias (thus not recognized as an Identifier)
        source_columns = SqlFluffColumn._extract_source_columns(column)
        return Column(column.raw, source_columns=source_columns)

    @staticmethod
    def _extract_source_columns(segment: BaseSegment) -> list[ColumnQualifierTuple]:
        """
        :param segment: segment to be processed
        :return: list of extracted source columns
        """
        col_list = []
        if segment.type in COLUMN_SEGMENT_TYPE or is_wildcard(segment):
            if cqt := extract_column_qualifier(segment):
                col_list = [cqt]
        elif segment.type in FUNCTION_SEGMENT_TYPE:
            for bracketed in segment.recursive_crawl("bracketed"):
                # the bracketed could be in function_contents or over_clause in case of window function
                col_list += SqlFluffColumn._get_column_from_parenthesis(bracketed)
        elif segment.type in NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE:
            sub_segments = list_child_segments(segment)
            col_list = []
            for sub_segment in sub_segments:
                if sub_segment.type == "bracketed":
                    if is_subquery(sub_segment):
                        col_list += SqlFluffColumn._get_column_from_subquery(
                            sub_segment
                        )
                    else:
                        col_list += SqlFluffColumn._get_column_from_parenthesis(
                            sub_segment
                        )
                elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                    sub_segment
                ):
                    res = SqlFluffColumn._extract_source_columns(sub_segment)
                    col_list.extend(res)
        return col_list

    @staticmethod
    def _get_column_from_subquery(
        sub_segment: BaseSegment,
    ) -> list[ColumnQualifierTuple]:
        """
        :param sub_segment: segment to be processed
        :return: A list of source columns from a segment
        """
        # 提取子查询中的所有表引用及其别名映射
        table_alias_map = {}
        for from_clause in sub_segment.recursive_crawl('from_clause'):
            # 处理FROM子句中的表
            for from_expr_element in from_clause.get_children('from_expression_element'):
                # 获取表引用和别名
                as_segment, target = extract_as_and_target_segment(from_expr_element)
                if not is_subquery(target):
                    table_reference = target.segments[0] if hasattr(target, 'segments') else target
                    table = SqlFluffTable.of(table_reference)
                    alias = extract_identifier(as_segment) if as_segment else None
                    full_table_name = f"{table.schema.name}.{table.raw_name}" if table.schema and table.schema.name else table.raw_name
                    if alias:
                        table_alias_map[alias] = full_table_name
                    table_alias_map[table.raw_name] = full_table_name
            
            # 处理JOIN子句中的表
            for join_clause in from_clause.get_children('join_clause'):
                for from_expr_element in join_clause.get_children('from_expression_element'):
                    as_segment, target = extract_as_and_target_segment(from_expr_element)
                    if not is_subquery(target):
                        table_reference = target.segments[0] if hasattr(target, 'segments') else target
                        table = SqlFluffTable.of(table_reference)
                        alias = extract_identifier(as_segment) if as_segment else None
                        full_table_name = f"{table.schema.name}.{table.raw_name}" if table.schema and table.schema.name else table.raw_name
                        if alias:
                            table_alias_map[alias] = full_table_name
                        table_alias_map[table.raw_name] = full_table_name
        
        if not table_alias_map:
            return []
        
        # 检查子查询的选择列表是否包含通配符
        select_clause = next(sub_segment.recursive_crawl('select_clause'), None)
        if not select_clause:
            return []
        
        wildcard_segments = list(select_clause.recursive_crawl('wildcard_expression'))
        
        source_columns = []
        
        # 如果有通配符，返回所有表的通配符引用
        if wildcard_segments:
            for alias, full_table_name in table_alias_map.items():
                source_columns.append(ColumnQualifierTuple('*', full_table_name))
        else:
            # 如果没有通配符，提取选择列表中的列
            for select_clause_element in select_clause.get_children('select_clause_element'):
                # 获取列名和别名
                source_cols, alias = SqlFluffColumn._get_column_and_alias(select_clause_element)
                if source_cols:
                    # 如果有源列，使用源列信息
                    source_columns.extend(source_cols)
                else:
                    # 否则，提取列引用
                    for column_reference in select_clause_element.recursive_crawl('column_reference'):
                        if cqt := extract_column_qualifier(column_reference):
                            # 如果有表别名，替换为完整表名
                            if cqt.qualifier and cqt.qualifier in table_alias_map:
                                source_columns.append(ColumnQualifierTuple(cqt.column, table_alias_map[cqt.qualifier]))
                            else:
                                source_columns.append(cqt)
        
        return source_columns

    @staticmethod
    def _get_column_from_parenthesis(
        sub_segment: BaseSegment,
    ) -> list[ColumnQualifierTuple]:
        # windows function has an extra layer, get rid of it so that it can be handled as regular functions
        if window_specification := sub_segment.get_child("window_specification"):
            sub_segment = window_specification
        col, _ = SqlFluffColumn._get_column_and_alias(sub_segment, False)
        return col if col else []

    @staticmethod
    def _get_column_and_alias(
        segment: BaseSegment, check_bracketed: bool = True
    ) -> tuple[list[ColumnQualifierTuple], str | None]:
        """
        check_bracketed is True for top-level column definition, like (col1 + col2) as col3
        set to False for bracket in function call, like coalesce(col1, col2) as col3
        """
        alias = None
        columns = []
        sub_segments = list_child_segments(segment, check_bracketed)
        for sub_segment in sub_segments:
            if sub_segment.type == "alias_expression":
                alias = extract_identifier(sub_segment)
            elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                sub_segment
            ):
                res = SqlFluffColumn._extract_source_columns(sub_segment)
                columns += res if res else []
        return columns, alias
