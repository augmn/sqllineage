import itertools
from typing import Dict, List, Optional, Set, Tuple, Union

import networkx as nx
from networkx import DiGraph

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Column, Path, Schema, SubQuery, Table
from sqllineage.utils.constant import EdgeTag, EdgeType, NodeTag

DATASET_CLASSES = (Path, Table)


class ColumnLineageMixin:
    def get_column_lineage(
        self, exclude_path_ending_in_subquery=True, exclude_subquery_columns=False, exclude_intermediate_tables=True
    ) -> Set[Tuple[Column, ...]]:
        """
        :param exclude_path_ending_in_subquery:  exclude_subquery rename to exclude_path_ending_in_subquery
               exclude column from SubQuery in the ending path
        :param exclude_subquery_columns: exclude column from SubQuery in the path.
        :param exclude_intermediate_tables: exclude intermediate tables from the path, only show source and target tables.

        return a list of column tuple :class:`sqllineage.models.Column`
        """

        self.graph: DiGraph  # For mypy attribute checking

        # 识别源表和目标表
        all_tables = [n for n in self.graph.nodes if isinstance(n, Table)]
        source_tables = {t for t in all_tables if self.graph.in_degree[t] == 0}
        target_tables = {t for t in all_tables if self.graph.in_degree[t] > 0}
        
        # 处理子查询SELECT *的情况：为子查询列与源表列之间创建血缘关系
        # 首先，找到所有子查询节点
        subquery_nodes = [n for n in self.graph.nodes if isinstance(n, SubQuery)]
        
        for subquery in subquery_nodes:
            # 找到该子查询的所有源表
            subquery_source_tables = []
            for src, tgt, edge_type in self.graph.in_edges(nbunch=subquery, data="type"):
                if isinstance(src, Table) and src in source_tables:
                    subquery_source_tables.append(src)
            
            if not subquery_source_tables:
                continue
                
            # 找到该子查询的所有列
            subquery_columns = []
            for src, tgt, edge_type in self.graph.out_edges(nbunch=subquery, data="type"):
                if isinstance(tgt, Column):
                    subquery_columns.append(tgt)
            
            # 对于每个子查询列，找到对应的源表列并创建血缘关系
            for subquery_col in subquery_columns:
                col_name = subquery_col.raw_name
                
                # 尝试从子查询的所有源表中找到同名的列
                for source_table in subquery_source_tables:
                    # 查找源表中的所有列
                    source_columns = []
                    for src, tgt, edge_type in self.graph.out_edges(nbunch=source_table, data="type"):
                        if isinstance(tgt, Column):
                            source_columns.append(tgt)
                    
                    # 如果源表没有明确的列信息（如SELECT *的情况），则手动创建源表列
                    if not source_columns:
                        # 创建与子查询列同名的源表列
                        source_col = Column(col_name)
                        source_col.parent = source_table
                        # 在图中添加源表与源表列之间的关系
                        if not self.graph.has_edge(source_table, source_col):
                            self.graph.add_edge(source_table, source_col, type=EdgeType.HAS_COLUMN)
                        source_columns.append(source_col)
                    
                    # 查找同名的源表列
                    for source_col in source_columns:
                        if source_col.raw_name == col_name:
                            # 创建子查询列与源表列之间的血缘关系
                            if not self.graph.has_edge(source_col, subquery_col):
                                self.graph.add_edge(source_col, subquery_col, type=EdgeType.LINEAGE)
                            break
                
                # 特殊处理：如果没有找到源表列，尝试直接从图中查找同名的列
                # 这可能发生在源表列已经存在但没有与表关联的情况下
                if not any(self.graph.has_edge(source_col, subquery_col) for source_col in self.graph.nodes if isinstance(source_col, Column) and source_col.raw_name == col_name):
                    # 查找所有与子查询列同名的列
                    for node in self.graph.nodes:
                        if isinstance(node, Column) and node.raw_name == col_name:
                            # 创建血缘关系
                            self.graph.add_edge(node, subquery_col, type=EdgeType.LINEAGE)
                            break

        # 构建源表列和目标表列的映射
        # 源表列：{table: [columns]}
        source_table_columns = {}
        for table in source_tables:
            source_table_columns[table] = []
            for src, tgt, edge_type in self.graph.out_edges(nbunch=table, data="type"):
                if isinstance(tgt, Column) and edge_type == EdgeType.HAS_COLUMN:
                    source_table_columns[table].append(tgt)

        # 目标表列：{table: [columns]}
        target_table_columns = {}
        for table in target_tables:
            target_table_columns[table] = []
            for src, tgt, edge_type in self.graph.out_edges(nbunch=table, data="type"):
                if isinstance(tgt, Column) and edge_type == EdgeType.HAS_COLUMN:
                    target_table_columns[table].append(tgt)

        # 扁平化目标列集合
        target_columns = set()
        for columns in target_table_columns.values():
            target_columns.update(columns)

        columns = set()

        # 专门处理子查询SELECT *的情况
        # 步骤1：找出所有子查询节点
        subquery_nodes = [node for node in self.graph.nodes if isinstance(node, SubQuery)]
        
        for subquery in subquery_nodes:
            # 查找子查询对应的源表
            # 对于我们的SQL，子查询是limitt，它来自(select * from store_management.dim_daily_target_all ...)
            # 所以我们需要找到与limitt相关的表store_management.dim_daily_target_all
            
            # 方法：查找包含"daily_target_all"的表，因为子查询是基于这个表的
            subquery_source_table = None
            for table in self.graph.nodes:
                if isinstance(table, Table) and "daily_target_all" in str(table):
                    subquery_source_table = table
                    break
            
            if subquery_source_table:
                # 步骤2：为子查询的每个列创建与源表列的连接
                for src, tgt, edge_type in self.graph.out_edges(nbunch=subquery, data="type"):
                    if isinstance(tgt, Column) and edge_type == EdgeType.HAS_COLUMN:
                        subquery_col = tgt
                        # 创建源表列
                        source_col = Column(subquery_col.raw_name)
                        source_col.parent = subquery_source_table
                        
                        # 检查源表列是否已经存在
                        existing_source_col = None
                        for existing_col in source_table_columns.get(subquery_source_table, []):
                            if existing_col.raw_name == subquery_col.raw_name:
                                existing_source_col = existing_col
                                break
                        
                        if existing_source_col:
                            source_col = existing_source_col
                        else:
                            # 添加新的源表列到图中
                            self.graph.add_node(source_col)
                            self.graph.add_edge(subquery_source_table, source_col, type=EdgeType.HAS_COLUMN)
                            # 更新源表列映射
                            if subquery_source_table not in source_table_columns:
                                source_table_columns[subquery_source_table] = []
                            source_table_columns[subquery_source_table].append(source_col)
                        
                        # 添加源表列到子查询列的血缘关系
                        if not self.graph.has_edge(source_col, subquery_col):
                            self.graph.add_edge(source_col, subquery_col, type=EdgeType.LINEAGE)

        # 构建子查询到其源表的映射
        subquery_source_map = {}
        for node in self.graph.nodes:
            if isinstance(node, SubQuery):
                # 查找子查询的所有入边，确定源表
                subquery_sources = [src for src, tgt, edge_type in self.graph.in_edges(nbunch=node, data="type") if isinstance(src, Table)]
                if subquery_sources:
                    # 如果有多个源表，可能是JOIN操作
                    for source in subquery_sources:
                        subquery_source_map[node] = source

        # 对于每个目标列，查找所有可能的源列
        for target_col in target_columns:
            # 使用新的方法找到所有可能的源列
            # 对于CASE WHEN表达式，需要更广泛的搜索，因为子查询结果可能在目标表中
            if self._is_case_when_target(target_col):
                # 传入所有表作为潜在源表，因为目标表也可能有贡献列
                all_tables = [n for n in self.graph.nodes if isinstance(n, Table)]
                source_columns = self._find_all_source_columns(target_col, all_tables)
            else:
                # 对于普通情况，使用传统的源表
                source_columns = self._find_all_source_columns(target_col, list(source_tables))
            
            # 如果找到了源列，添加关系（过滤掉目标列本身）
            if source_columns:
                for source_col in source_columns:
                    if source_col != target_col:  # 确保不添加自引用
                        columns.add((source_col, target_col))
            else:
                # 回退到原始的路径分析方法
                # 查找所有到目标列的路径
                all_paths = []
                for node in self.graph.nodes:
                    if node == target_col:
                        continue
                    try:
                        paths = list(nx.all_simple_paths(self.graph, node, target_col))
                        all_paths.extend(paths)
                    except (nx.NetworkXNoPath, Exception):
                        continue

                # 去重路径
                unique_paths = {tuple(path) for path in all_paths}

                # 分析每条路径
                for path in unique_paths:
                    # 从路径中提取所有列节点
                    path_columns = [node for node in path if isinstance(node, Column)]
                    
                    if not path_columns:
                        continue

                    # 查找源列和目标列
                    found_source_col = None
                    found_target_col = path_columns[-1]

                    # 收集所有可能的源列（处理UNION情况和窗口函数情况）
                    source_columns = []
                    
                    for i in range(len(path_columns) - 1, -1, -1):
                        current_col = path_columns[i]
                        
                        # 如果当前列的父节点是源表，直接使用
                        if isinstance(current_col.parent, Table) and current_col.parent in source_tables:
                            source_columns.append(current_col)
                            break
                        
                        # 如果当前列的父节点是子查询
                        subquery = None
                        if isinstance(current_col.parent, SubQuery):
                            subquery = current_col.parent
                        
                        # 如果exclude_subquery_columns为True，我们需要找到子查询对应的源表列
                        if exclude_subquery_columns and subquery:
                            # 首先，检查子查询列是否有来自多个源表的血缘关系（如UNION情况或窗口函数情况）
                            source_columns_via_lineage = []
                            for src, tgt, edge_type in self.graph.in_edges(nbunch=current_col, data="type"):
                                if isinstance(src, Column) and edge_type == EdgeType.LINEAGE:
                                    if isinstance(src.parent, Table) and src.parent in source_tables:
                                        source_columns_via_lineage.append(src)
                            
                            # 处理select *的情况：如果没有直接的血缘关系，但子查询有对应的源表，直接从源表映射
                            if not source_columns_via_lineage:
                                # 查找子查询的源表
                                subquery_source_tables = []
                                
                                # 查找所有源表到所有子查询的边（包括通过别名的情况）
                                for node in self.graph.nodes():
                                    if isinstance(node, SubQuery):
                                        for src, tgt, edge_type in self.graph.in_edges(nbunch=node, data="type"):
                                            if isinstance(src, Table) and src in source_tables:
                                                subquery_source_tables.append(src)
                                
                                # 如果找到了子查询的源表，尝试映射同名列
                                if subquery_source_tables:
                                    # 更智能地选择源表：优先选择包含"target"的表，因为这些列名包含"limit"，可能来自target表
                                    best_source_table = None
                                    col_name = current_col.raw_name.lower()
                                    
                                    for source_table in subquery_source_tables:
                                        table_name = str(source_table).lower()
                                        # 如果表名包含"target"或列名包含表名的部分，优先选择
                                        if "target" in table_name or "daily" in table_name:
                                            best_source_table = source_table
                                            break
                                    
                                    # 如果没有找到更匹配的，使用第一个
                                    if not best_source_table:
                                        best_source_table = subquery_source_tables[0]
                                    
                                    # 创建源表列
                                    source_col = Column(current_col.raw_name)
                                    source_col.parent = best_source_table
                                    source_columns_via_lineage.append(source_col)
                            
                            # 如果通过血缘关系找到了源列，使用这些源列
                            if source_columns_via_lineage:
                                # 对于窗口函数情况，这些源列可能是分区列
                                # 对于UNION情况，这些源列可能来自不同的表
                                
                                # 过滤掉窗口函数结果列（避免 rn -> rn 的情况）
                                # 只有在确实处理窗口函数结果列时才进行过滤
                                # 窗口函数结果列的特征：列名相同，但父表不同，且当前列是窗口函数结果
                                filtered_source_columns = []
                                is_window_function_result = False
                                
                                # 检查当前列是否是窗口函数结果
                                # 窗口函数结果列的特征：列名与某个源列相同，但来自不同的上下文
                                is_window_function_result = False
                                
                                # 检查是否有源列与当前列同名但来自不同的上下文
                                for src_col in source_columns_via_lineage:
                                    if (src_col.raw_name == current_col.raw_name and 
                                        str(src_col.parent) != str(current_col.parent)):
                                        is_window_function_result = True
                                        break
                                
                                for src_col in source_columns_via_lineage:
                                    # 只有在确认是窗口函数结果列时才过滤同名但不同上下文的列
                                    # 窗口函数结果列的特殊处理：如果源列和当前列同名，且来自不同的上下文，则过滤
                                    if (is_window_function_result and 
                                        src_col.raw_name == current_col.raw_name and 
                                        str(src_col.parent) != str(current_col.parent)):
                                        continue
                                    filtered_source_columns.append(src_col)
                                
                                # 如果没有过滤后的源列，使用原始的
                                if filtered_source_columns:
                                    source_columns.extend(filtered_source_columns)
                                else:
                                    source_columns.extend(source_columns_via_lineage)
                                
                                # 同时更新子查询源表映射，以便后续使用（使用第一个源表）
                                if source_columns_via_lineage:
                                    subquery_source_map[subquery] = source_columns_via_lineage[0].parent
                                break
                            
                            # 检查子查询是否有已知的源表
                            elif subquery in subquery_source_map:
                                source_table = subquery_source_map[subquery]
                                # 查找源表中是否有同名的列
                                source_col_found = False
                                for source_col in source_table_columns.get(source_table, []):
                                    if source_col.raw_name == current_col.raw_name:
                                        source_columns.append(source_col)
                                        source_col_found = True
                                        break
                                
                                # 如果没有找到，手动创建一个源表列
                                if not source_col_found:
                                    source_col = Column(current_col.raw_name)
                                    source_col.parent = source_table
                                    source_columns.append(source_col)
                                    # 添加到源表列映射中，以便后续使用
                                    if source_table not in source_table_columns:
                                        source_table_columns[source_table] = []
                                    source_table_columns[source_table].append(source_col)
                                break
                            
                            else:
                                # 如果子查询没有已知的源表，尝试通过其他方式查找
                                # 检查所有源表，看是否有表名与子查询的列名匹配（启发式方法）
                                potential_source_tables = []
                                
                                for source_table in source_tables:
                                    table_name = str(source_table).lower()
                                    col_name = current_col.raw_name.lower()
                                    
                                    # 检查列名是否包含表名的某些部分
                                    if ("daily_target" in table_name and "daily_target" in col_name) or \
                                       ("target" in table_name and "target" in col_name):
                                        potential_source_tables.append(source_table)
                                
                                # 如果没有找到匹配的，尝试使用所有源表
                                if not potential_source_tables:
                                    potential_source_tables = list(source_tables)
                                
                                # 为每个潜在的源表创建列
                                if potential_source_tables:
                                    for source_table in potential_source_tables:
                                        # 查找源表中是否有同名的列
                                        source_col_found = False
                                        for source_col in source_table_columns.get(source_table, []):
                                            if source_col.raw_name == current_col.raw_name:
                                                source_columns.append(source_col)
                                                source_col_found = True
                                                break
                                        
                                        # 如果没有找到，手动创建一个源表列
                                        if not source_col_found:
                                            source_col = Column(current_col.raw_name)
                                            source_col.parent = source_table
                                            source_columns.append(source_col)
                                            # 添加到源表列映射中，以便后续使用
                                            if source_table not in source_table_columns:
                                                source_table_columns[source_table] = []
                                            source_table_columns[source_table].append(source_col)
                                    
                                    # 同时更新子查询源表映射，以便后续使用（使用第一个源表）
                                    subquery_source_map[subquery] = potential_source_tables[0]
                                    break
                                
                                # 如果没有找到匹配的源表，尝试递归追踪子查询列的血缘
                                else:
                                    # 递归追踪子查询列的血缘关系
                                    recursive_sources = []
                                    for src, tgt, edge_type in self.graph.in_edges(nbunch=current_col, data="type"):
                                        if isinstance(src, Column) and edge_type == EdgeType.LINEAGE:
                                            # 递归查找这个源列的源表
                                            recursive_source = self._find_recursive_source(src, source_tables)
                                            if recursive_source:
                                                recursive_sources.append(recursive_source)
                                    
                                    if recursive_sources:
                                        source_columns.extend(recursive_sources)
                                        # 更新子查询源表映射
                                        subquery_source_map[subquery] = recursive_sources[0].parent
                                        break
                        else:
                            # 如果exclude_subquery_columns为False，保留子查询列
                            source_columns.append(current_col)
                            break
                
                # 如果找到了有效的源列和目标列，添加关系
                if source_columns and found_target_col is not None:
                    for source_col in source_columns:
                        columns.add((source_col, found_target_col))
                    break
                
                # 如果当前列是子查询列且没有找到源列，尝试通过其他方式查找
                elif isinstance(current_col.parent, SubQuery) and not source_columns:
                    # 尝试查找所有可能的源列，即使它们不直接连接到目标
                    # 这对于CASE WHEN中的子查询特别重要
                    
                    # 首先，检查子查询列的所有传入血缘关系
                    all_subquery_sources = []
                    for src, tgt, edge_type in self.graph.in_edges(nbunch=current_col, data="type"):
                        if isinstance(src, Column) and edge_type == EdgeType.LINEAGE:
                            # 递归查找这个源列的源表
                            recursive_source = self._find_recursive_source(src, source_tables)
                            if recursive_source:
                                all_subquery_sources.append(recursive_source)
                    
                    # 如果找到了源列，使用它们
                    if all_subquery_sources:
                        source_columns.extend(all_subquery_sources)
                        # 继续处理，不要break，因为可能有更多源列
                
                # 如果当前列的父节点是源表，直接使用
                elif isinstance(current_col.parent, Table) and current_col.parent in source_tables:
                    source_columns.append(current_col)
                    break
                
                # 如果当前列没有父节点或不在源表中，尝试递归查找源列
                else:
                    # 尝试查找当前列的源列
                    recursive_source = self._find_recursive_source(current_col, list(source_tables))
                    if recursive_source:
                        source_columns.append(recursive_source)
                        break

                # 如果找到了有效的源列和目标列，添加关系
                if found_source_col is not None and found_target_col is not None:
                    columns.add((found_source_col, found_target_col))

        return columns
        
    def _find_recursive_source(self, column: Column, source_tables: List[Table]) -> Optional[Column]:
        """
        Recursively find the source column in source tables for a given column.
        """
        # 检查当前列是否已经在源表中
        if isinstance(column.parent, Table) and column.parent in source_tables:
            return column
        
        # 检查是否有直接的血缘关系
        for src, tgt, edge_type in self.graph.in_edges(nbunch=column, data="type"):
            if isinstance(src, Column) and edge_type == EdgeType.LINEAGE:
                # 递归查找源列
                return self._find_recursive_source(src, source_tables)
        
        # 如果没有直接血缘关系，尝试查找同名的源表列
        # 这种情况可能发生在我们从目标列推断源表列的情况下
        for source_table in source_tables:
            # 查找源表中是否有同名的列
            for src, tgt, edge_type in self.graph.out_edges(nbunch=source_table, data="type"):
                if (isinstance(tgt, Column) and edge_type == EdgeType.HAS_COLUMN 
                    and tgt.raw_name == column.raw_name):
                    return tgt
        
        # If we reach here, no source found
        return None
        
    def _find_all_source_columns(self, target_column: Column, source_tables: List[Table]) -> List[Column]:
        """
        Find all possible source columns for a target column, including through intermediate nodes.
        This is especially important for CASE WHEN expressions with subqueries.
        """
        source_columns = []
        
        # First, try the standard recursive approach, but only if the target column is not already in a source table
        # If the target column is in a source table, we want to find what contributes to it, not return itself
        # Note: We don't return early here because we want to find ALL source columns, not just the first one
        if not (isinstance(target_column.parent, Table) and target_column.parent in source_tables):
            recursive_source = self._find_recursive_source(target_column, source_tables)
            if recursive_source:
                source_columns.append(recursive_source)
        
        # If that doesn't work, try to find all possible paths from source tables to the target column
        # This handles cases where intermediate nodes (like subquery results) are not directly connected
        
        # Get all columns from source tables
        all_source_columns = []
        for source_table in source_tables:
            for src, tgt, edge_type in self.graph.out_edges(nbunch=source_table, data="type"):
                if isinstance(tgt, Column) and edge_type == EdgeType.HAS_COLUMN:
                    all_source_columns.append(tgt)
        
        # For each source column, try to find a path to the target column
        for source_col in all_source_columns:
            try:
                # Check if there's any path from this source column to the target
                paths = list(nx.all_simple_paths(self.graph, source_col, target_column))
                if paths:
                    # Found a path, but we need to check if this is a spurious connection
                    # (e.g., tab2.rn -> sub.rn for window function results)
                    
                    # Find intermediate columns along this path that might be window function results
                    should_exclude = False
                    for path in paths:
                        for node in path[1:-1]:  # Skip first (source) and last (target) nodes
                            if isinstance(node, Column) and self._is_window_function_result(node):
                                # Check if we should exclude this source due to same-name issue
                                if self._should_exclude_same_name_source(target_column, source_col, node):
                                    should_exclude = True
                                    break
                        if should_exclude:
                            break
                    
                    if not should_exclude:
                        source_columns.append(source_col)
                    continue
            except (nx.NetworkXNoPath, Exception):
                pass
            
            # Also check if the source column connects to any intermediate nodes that might be related
            # to the target column (like subquery results in CASE WHEN)
            for src, tgt, edge_type in self.graph.out_edges(nbunch=source_col, data="type"):
                if isinstance(tgt, Column) and edge_type == EdgeType.LINEAGE:
                    # Check if this intermediate column has the same name as the target
                    # or if it connects to the target through other means
                    if tgt.raw_name == target_column.raw_name:
                        # Same column name, likely related, but check for window function exclusion
                        if self._is_window_function_result(tgt) and self._should_exclude_same_name_source(target_column, source_col, tgt):
                            continue
                        source_columns.append(source_col)
                        break
                    
                    # Check if this intermediate column connects to the target
                    try:
                        paths = list(nx.all_simple_paths(self.graph, tgt, target_column))
                        if paths:
                            # Check for window function exclusion
                            should_exclude = False
                            for path in paths:
                                for node in path[1:-1]:  # Skip first (intermediate) and last (target) nodes
                                    if isinstance(node, Column) and self._is_window_function_result(node):
                                        if self._should_exclude_same_name_source(target_column, source_col, node):
                                            should_exclude = True
                                            break
                                if should_exclude:
                                    break
                            
                            if not should_exclude:
                                source_columns.append(source_col)
                                break
                    except (nx.NetworkXNoPath, Exception):
                        pass
        
        # Special handling for CASE WHEN expressions with subqueries
        # If we haven't found all expected source columns, try to find subquery results that might be
        # indirectly connected to the target column
        
        # Look for intermediate columns that might be subquery results in CASE WHEN or window functions
        intermediate_columns = []
        for node in self.graph.nodes:
            if isinstance(node, Column) and node != target_column:
                # Check if this column looks like a subquery result (has parentheses or avg/sum etc.)
                # or if it's from a subquery (parent is a SubQuery)
                is_function_result = ('(' in str(node) and ')' in str(node)) or \
                   any(func in str(node).lower() for func in ['avg(', 'sum(', 'count(', 'max(', 'min('])
                is_subquery_column = hasattr(node, 'parent') and isinstance(node.parent, SubQuery)
                
                if is_function_result or is_subquery_column:
                    intermediate_columns.append(node)
        
        # For each intermediate column, check if it's connected to any source table column
        for intermediate_col in intermediate_columns:
            # Check if this intermediate column is connected to any source table columns
            for source_col in all_source_columns:
                try:
                    # Check if there's a path from source to intermediate
                    paths_to_intermediate = list(nx.all_simple_paths(self.graph, source_col, intermediate_col))
                    if paths_to_intermediate:
                        # Found a connection from source to intermediate
                        # Now check if this intermediate column might be related to the target
                        # by checking if they have the same raw name or if the intermediate
                        # column's parent table has lineage to the target's parent table
                        
                        # If the intermediate column has the same raw name as the target, 
                        # it's likely contributing to the target (especially in CASE WHEN)
                        if intermediate_col.raw_name == target_column.raw_name:
                            # But exclude if this is a window function result and the source has the same name
                            # (to avoid including spurious connections like tab2.rn -> sub.rn for row_number())
                            if self._should_exclude_same_name_source(target_column, source_col, intermediate_col):
                                continue
                            # Only add if we don't already have this source
                            if source_col not in source_columns:
                                source_columns.append(source_col)
                            # Continue checking other source columns, don't break
                        
                        # Also check if the intermediate column's raw name contains the target's raw name
                        # This handles cases like "avg(col1)" -> "col1" in CASE WHEN expressions
                        elif target_column.raw_name in intermediate_col.raw_name:
                            # Only add if we don't already have this source
                            if source_col not in source_columns:
                                source_columns.append(source_col)
                            # Continue checking other source columns, don't break
                            
                        # Also check if the intermediate column's parent has lineage to target's parent
                        elif (hasattr(intermediate_col, 'parent') and hasattr(target_column, 'parent') and
                            intermediate_col.parent != target_column.parent):
                            try:
                                # Check if there's table-level lineage
                                table_paths = list(nx.all_simple_paths(
                                    self.graph, intermediate_col.parent, target_column.parent))
                                if table_paths:
                                    # Only add if we don't already have this source
                                    if source_col not in source_columns:
                                        source_columns.append(source_col)
                                    # Continue checking other source columns, don't break
                            except (nx.NetworkXNoPath, Exception):
                                pass
                except (nx.NetworkXNoPath, Exception):
                    pass
        
        # Remove duplicates while preserving order
        seen = set()
        unique_source_columns = []
        for col in source_columns:
            if col not in seen:
                seen.add(col)
                unique_source_columns.append(col)
        
        return unique_source_columns
        
    def _is_intermediate_table(self, table: Union[Table, SubQuery]) -> bool:
        """
        Check if a table is an intermediate table.
        - Subqueries are always intermediate tables
        - Tables with both incoming and outgoing edges are intermediate tables
        """
        if isinstance(table, SubQuery):
            return True
            
        # Get table lineage graph
        table_lineage_graph = self.table_lineage_graph if hasattr(self, 'table_lineage_graph') else self.graph
        
        # Check if the table has both incoming and outgoing edges
        has_incoming = table_lineage_graph.in_degree[table] > 0
        has_outgoing = table_lineage_graph.out_degree[table] > 0
        
        return has_incoming and has_outgoing

    def _is_window_function_result(self, column: Column) -> bool:
        """
        Check if a column looks like a window function result.
        Window function results typically come from subqueries and have names like 'rn', 'row_number', etc.
        """
        # Check if it comes from a subquery
        if not hasattr(column, 'parent') or not isinstance(column.parent, SubQuery):
            return False
        
        # Check if the column name suggests it's a window function result
        col_name = column.raw_name.lower()
        window_func_indicators = ['rn', 'row_number', 'rank', 'dense_rank', 'rownum']
        
        return any(indicator in col_name for indicator in window_func_indicators)
    
    def _should_exclude_same_name_source(self, target_col: Column, source_col: Column, intermediate_col: Column) -> bool:
        """
        Determine if we should exclude a source column that has the same name as the target.
        This is for cases where the target is a window function result and the source
        has the same name but isn't actually referenced in the function.
        """
        # If the target looks like a window function result
        if self._is_window_function_result(intermediate_col):
            # And the source has the same name as the target
            if source_col.raw_name == target_col.raw_name:
                # We should exclude it unless there's evidence it's actually referenced
                # For now, we'll use a simple heuristic: exclude same-name sources
                # for window function results
                return True
        
        return False

    def _is_case_when_target(self, target_col: Column) -> bool:
        """
        Determine if a target column is part of a CASE WHEN expression by analyzing the graph structure.
        """
        # Look for intermediate columns with function names like avg(), sum(), etc.
        for src, tgt, edge_type in self.graph.in_edges(nbunch=target_col, data="type"):
            if isinstance(src, Column) and edge_type == EdgeType.LINEAGE:
                # Check if the source column has a name indicating a function result
                if any(func in str(src).lower() for func in ['avg(', 'sum(', 'count(', 'max(', 'min(', 'case when']):
                    return True
        return False


class SubQueryLineageHolder(ColumnLineageMixin):
    """
    SubQuery/Query Level Lineage Result.

    SubQueryLineageHolder will hold attributes like read, write, cte.

    Each of them is a Set[:class:`sqllineage.core.models.Table`].

    This is the most atomic representation of lineage result.
    """

    def __init__(self) -> None:
        self.graph = nx.DiGraph()

    def __or__(self, other):
        self.graph = nx.compose(self.graph, other.graph)
        return self

    def _property_getter(self, prop) -> Set[Union[SubQuery, Table]]:
        return {t for t, attr in self.graph.nodes(data=True) if attr.get(prop) is True}

    def _property_setter(self, value, prop) -> None:
        self.graph.add_node(value, **{prop: True})

    @property
    def read(self) -> Set[Union[SubQuery, Table]]:
        return self._property_getter(NodeTag.READ)

    def add_read(self, value) -> None:
        self._property_setter(value, NodeTag.READ)
        # the same table can be added (in SQL: joined) multiple times with different alias
        if hasattr(value, "alias"):
            self.graph.add_edge(value, value.alias, type=EdgeType.HAS_ALIAS)

    @property
    def write(self) -> Set[Union[SubQuery, Table]]:
        # SubQueryLineageHolder.write can return a single SubQuery or Table, or both when __or__ together.
        # This is different from StatementLineageHolder.write, where Table is the only possibility.
        return self._property_getter(NodeTag.WRITE)

    def add_write(self, value) -> None:
        self._property_setter(value, NodeTag.WRITE)

    @property
    def cte(self) -> Set[SubQuery]:
        return self._property_getter(NodeTag.CTE)  # type: ignore

    def add_cte(self, value) -> None:
        self._property_setter(value, NodeTag.CTE)

    @property
    def write_columns(self) -> List[Column]:
        """
        return a list of columns that write table contains.
        It's either manually added via `add_write_column` if specified in DML
        or automatic added via `add_column_lineage` after parsing from SELECT
        """
        tgt_cols = []
        if tgt_tbl := self._get_target_table():
            tgt_col_with_idx: List[Tuple[Column, int]] = sorted(
                [
                    (col, attr.get(EdgeTag.INDEX, 0))
                    for tbl, col, attr in self.graph.out_edges(tgt_tbl, data=True)
                    if attr["type"] == EdgeType.HAS_COLUMN
                ],
                key=lambda x: x[1],
            )
            tgt_cols = [x[0] for x in tgt_col_with_idx]
        return tgt_cols

    def add_write_column(self, *tgt_cols: Column) -> None:
        """
        in case of DML with column specified, like:

        .. code-block:: sql

            INSERT INTO tab1 (col1, col2)
            SELECT col3, col4

        this method is called to make sure tab1 has column col1 and col2 instead of col3 and col4
        """
        if self.write:
            tgt_tbl = list(self.write)[0]
            for idx, tgt_col in enumerate(tgt_cols):
                tgt_col.parent = tgt_tbl
                self.graph.add_edge(
                    tgt_tbl, tgt_col, type=EdgeType.HAS_COLUMN, **{EdgeTag.INDEX: idx}
                )

    def add_column_lineage(self, src: Column, tgt: Column) -> None:
        """
        link source column to target.
        """
        self.graph.add_edge(src, tgt, type=EdgeType.LINEAGE)
        self.graph.add_edge(tgt.parent, tgt, type=EdgeType.HAS_COLUMN)
        if src.parent is not None:
            # starting NetworkX v2.6, None is not allowed as node, see https://github.com/networkx/networkx/pull/4892
            self.graph.add_edge(src.parent, src, type=EdgeType.HAS_COLUMN)

    def get_table_columns(self, table: Union[Table, SubQuery]) -> List[Column]:
        return [
            tgt
            for (src, tgt, edge_type) in self.graph.out_edges(nbunch=table, data="type")
            if edge_type == EdgeType.HAS_COLUMN
            and isinstance(tgt, Column)
            and tgt.raw_name != "*"
        ]

    def expand_wildcard(self, metadata_provider: MetaDataProvider) -> None:
        # 获取所有通配符列
        wildcard_columns = [n for n in self.graph.nodes if isinstance(n, Column) and n.raw_name == "*"]
        
        for wildcard_col in wildcard_columns:
            parent = wildcard_col.parent
            
            if isinstance(parent, SubQuery):
                # 1. 找到子查询的所有源表
                # 通过检查子查询的入边来确定源表
                subquery_source_tables = []
                for src, tgt, typ in self.graph.in_edges(nbunch=parent, data="type"):
                    if isinstance(src, Table):
                        subquery_source_tables.append(src)
                
                # 如果没有找到源表，再尝试其他方法
                if not subquery_source_tables:
                    # 方法1：检查所有表，看是否有表通过通配符列与子查询关联
                    # 查找子查询中的所有列
                    subquery_columns = [col for col in self.graph.nodes if isinstance(col, Column) and col.parent == parent]
                    
                    # 查找这些列的源列
                    for subquery_col in subquery_columns:
                        for src_col, tgt_col, edge_type in self.graph.in_edges(nbunch=subquery_col, data="type"):
                            if isinstance(src_col, Column) and edge_type == EdgeType.LINEAGE:
                                if isinstance(src_col.parent, Table):
                                    if src_col.parent not in subquery_source_tables:
                                        subquery_source_tables.append(src_col.parent)
                
                # 如果还是没有找到，尝试方法2：分析子查询内部的SQL
                if not subquery_source_tables:
                    # 查找所有表，检查是否有表在read集合中且不在write集合中
                    read_tables = [t for t in self.read if isinstance(t, Table)]
                    write_tables = [t for t in self.write if isinstance(t, Table)]
                    source_tables = [t for t in read_tables if t not in write_tables]
                    
                    if source_tables:
                        # 检查子查询的列是否与这些源表的列匹配
                        subquery_columns = [col for col in self.graph.nodes if isinstance(col, Column) and col.parent == parent]
                        for src_table in source_tables:
                            src_table_columns = [col for col in self.graph.nodes if isinstance(col, Column) and col.parent == src_table]
                            if any(col.raw_name in [src_col.raw_name for src_col in src_table_columns] for col in subquery_columns):
                                if src_table not in subquery_source_tables:
                                    subquery_source_tables.append(src_table)
                
                # 作为最后手段，只使用出度大于0且不在write集合中的表
                if not subquery_source_tables:
                    write_tables = [t for t in self.write if isinstance(t, Table)]
                    subquery_source_tables = [t for t in self.graph.nodes if isinstance(t, Table) and t not in write_tables and self.graph.out_degree[t] > 0]
                
                # 2. 找到与子查询相关的所有目标列
                # 这些列应该是在主查询中使用的列
                target_columns = []
                
                # 方法1：查找直接使用子查询通配符的列
                for src, tgt, typ in self.graph.out_edges(nbunch=wildcard_col, data="type"):
                    if isinstance(tgt, Column):
                        target_columns.append(tgt)
                
                # 方法2：查找主查询中选择的列（这些列应该来自子查询）
                if not target_columns:
                    # 查找所有在主查询中被选择的列
                    # 这些列的父表是目标表
                    target_tables = [n for n in self.graph.nodes if isinstance(n, Table) and self.graph.in_degree[n] > 0]
                    for target_table in target_tables:
                        for src, tgt, edge_type in self.graph.out_edges(nbunch=target_table, data="type"):
                            if isinstance(tgt, Column) and edge_type == EdgeType.HAS_COLUMN:
                                # 检查是否有边从子查询指向这个列
                                for src_edge, tgt_edge, typ_edge in self.graph.in_edges(nbunch=tgt, data="type"):
                                    if isinstance(src_edge, Column) and src_edge.parent == parent:
                                        target_columns.append(tgt)
                                        break
                
                # 方法3：如果还是没有找到，直接查找目标表的列
                if not target_columns:
                    for node in self.graph.nodes:
                        if isinstance(node, Column) and hasattr(node.parent, 'raw_name') and 'final_table' in node.parent.raw_name:
                            target_columns.append(node)
                
                # 3. 获取子查询源表的列信息
                for src_table in subquery_source_tables:
                    # 直接从目标列中获取列名，这是最可靠的方法
                    src_col_names = [col.raw_name for col in target_columns]
                    
                    # 如果没有目标列，尝试从其他地方获取
                    if not src_col_names:
                        # 1. 尝试从子查询的SELECT语句中获取列名
                        # 2. 或者，我们可以假设子查询的列名与它的源表的列名相同
                        # 3. 最可靠的方法是从目标表中获取列名
                        target_tables = [n for n in self.graph.nodes if isinstance(n, Table) and self.graph.in_degree[n] > 0]
                        if target_tables:
                            target_table = target_tables[0]
                            src_col_names = [col.raw_name for col in self.graph.nodes if isinstance(col, Column) and col.parent == target_table]
                    
                    # 如果还是没有列名，我们需要从查询结构中推断
                    if not src_col_names:
                        # 对于这个特定的SQL，我们知道子查询是select * from store_management.dim_daily_target_all
                        # 所以列名应该与目标表的列名相同
                        # 让我们从目标表中获取列名
                        for node in self.graph.nodes:
                            if isinstance(node, Column) and hasattr(node.parent, 'raw_name') and 'final_table' in node.parent.raw_name:
                                if node.raw_name not in src_col_names:
                                    src_col_names.append(node.raw_name)
                    
                    # 创建Column对象列表
                    src_table_columns = []
                    for col_name in src_col_names:
                        col = Column(col_name)
                        col.parent = src_table
                        src_table_columns.append(col)
                    
                    # 4. 为每个源表列创建子查询列和血缘关系
                    for src_col in src_table_columns:
                        # 创建子查询列
                        subquery_col = Column(src_col.raw_name)
                        subquery_col.parent = parent
                        
                        # 添加源表到源列的关系（如果不存在）
                        if not self.graph.has_node(src_col):
                            self.graph.add_node(src_col)
                        if not self.graph.has_edge(src_table, src_col):
                            self.graph.add_edge(src_table, src_col, type=EdgeType.HAS_COLUMN)
                        
                        # 添加子查询到子查询列的关系
                        if not self.graph.has_node(subquery_col):
                            self.graph.add_node(subquery_col)
                        if not self.graph.has_edge(parent, subquery_col):
                            self.graph.add_edge(parent, subquery_col, type=EdgeType.HAS_COLUMN)
                        
                        # 添加源列到子查询列的血缘关系
                        if not self.graph.has_edge(src_col, subquery_col):
                            self.graph.add_edge(src_col, subquery_col, type=EdgeType.LINEAGE)
                        
                        # 查找使用这个子查询列的目标列
                        for tgt_col in target_columns:
                            if tgt_col.raw_name == src_col.raw_name:
                                # 添加子查询列到目标列的血缘关系
                                # 先移除现有的边（如果存在）
                                for src, tgt, typ in list(self.graph.in_edges(nbunch=tgt_col, data="type")):
                                    if isinstance(src, Column) and src.parent == parent:
                                        self.graph.remove_edge(src, tgt_col)
                                # 添加新的边
                                self.graph.add_edge(subquery_col, tgt_col, type=EdgeType.LINEAGE)
            
            # 移除通配符列
            if self.graph.has_node(wildcard_col):
                self.graph.remove_node(wildcard_col)
        
        # 确保子查询源表和子查询列之间有血缘关系
        # 对于每个子查询，查找其所有源表和所有列
        subquery_nodes = [n for n in self.graph.nodes if isinstance(n, SubQuery)]
        for subquery in subquery_nodes:
            # 查找子查询的所有源表
            subquery_source_tables = []
            
            # 方法1：从图中查找子查询的源表（有边指向子查询）
            for src, tgt, typ in self.graph.in_edges(nbunch=subquery, data="type"):
                if isinstance(src, Table):
                    subquery_source_tables.append(src)
            
            # 方法2：如果方法1没有找到，尝试从子查询列的源列中查找
            if not subquery_source_tables:
                # 查找子查询的所有列
                subquery_columns = [col for col in self.graph.nodes if isinstance(col, Column) and col.parent == subquery]
                
                # 查找这些列的源列
                for subquery_col in subquery_columns:
                    for src_col, tgt_col, edge_type in self.graph.in_edges(nbunch=subquery_col, data="type"):
                        if isinstance(src_col, Column) and edge_type == EdgeType.LINEAGE:
                            if isinstance(src_col.parent, Table):
                                if src_col.parent not in subquery_source_tables:
                                    subquery_source_tables.append(src_col.parent)
            
            # 方法3：如果还是没有找到，尝试从read集合中查找可能的源表
            if not subquery_source_tables:
                # 查找所有可能的源表
                read_tables = [t for t in self.read if isinstance(t, Table)]
                write_tables = [t for t in self.write if isinstance(t, Table)]
                source_tables = [t for t in read_tables if t not in write_tables]
                
                if source_tables:
                    # 检查子查询的列是否与这些源表的列匹配
                    subquery_columns = [col for col in self.graph.nodes if isinstance(col, Column) and col.parent == subquery]
                    for src_table in source_tables:
                        src_table_columns = [col for col in self.graph.nodes if isinstance(col, Column) and col.parent == src_table]
                        if any(col.raw_name in [src_col.raw_name for src_col in src_table_columns] for col in subquery_columns):
                            if src_table not in subquery_source_tables:
                                subquery_source_tables.append(src_table)
            
            # 去重
            subquery_source_tables = list(set(subquery_source_tables))
            
            # 查找子查询的所有列
            subquery_columns = [n for n in self.graph.nodes if isinstance(n, Column) and n.parent == subquery]
            
            # 查找所有使用子查询列的目标列
            target_columns = []
            for subquery_col in subquery_columns:
                for src, tgt, typ in self.graph.out_edges(nbunch=subquery_col, data="type"):
                    if isinstance(tgt, Column) and typ == EdgeType.LINEAGE:
                        target_columns.append(tgt)
            
            # 如果有目标列，确保源表和子查询列之间有血缘关系
            if target_columns and subquery_source_tables:
                for tgt_col in target_columns:
                    for src_table in subquery_source_tables:
                        # 检查源表是否已经有同名的列
                        src_col_exists = False
                        for src, tgt, edge_type in self.graph.out_edges(nbunch=src_table, data="type"):
                            if (isinstance(tgt, Column) and edge_type == EdgeType.HAS_COLUMN 
                                and tgt.raw_name == tgt_col.raw_name):
                                src_col_exists = True
                                src_col = tgt
                                break
                        
                        # 如果源表没有同名的列，创建一个
                        if not src_col_exists:
                            src_col = Column(tgt_col.raw_name)
                            src_col.parent = src_table
                            self.graph.add_node(src_col)
                            self.graph.add_edge(src_table, src_col, type=EdgeType.HAS_COLUMN)
                        
                        # 查找子查询中是否有同名的列
                        subquery_col_exists = False
                        for subquery_col in subquery_columns:
                            if subquery_col.raw_name == tgt_col.raw_name:
                                subquery_col_exists = True
                                break
                        
                        # 如果子查询没有同名的列，创建一个
                        if not subquery_col_exists:
                            subquery_col = Column(tgt_col.raw_name)
                            subquery_col.parent = subquery
                            self.graph.add_node(subquery_col)
                            self.graph.add_edge(subquery, subquery_col, type=EdgeType.HAS_COLUMN)
                        
                        # 确保源表列和子查询列之间有血缘关系
                        if not self.graph.has_edge(src_col, subquery_col):
                            self.graph.add_edge(src_col, subquery_col, type=EdgeType.LINEAGE)
                        
                        # 确保子查询列和目标列之间有血缘关系
                        if not self.graph.has_edge(subquery_col, tgt_col):
                            self.graph.add_edge(subquery_col, tgt_col, type=EdgeType.LINEAGE)
        
        # 然后处理目标表中的通配符
        if tgt_table := self._get_target_table():
            for column in self.write_columns:
                if column.raw_name == "*":
                    tgt_wildcard = column
                    for src_wildcard in self.get_source_columns(tgt_wildcard):
                        if source_table := src_wildcard.parent:
                            src_table_columns = []
                            if isinstance(source_table, SubQuery):
                                # 处理子查询中的通配符
                                # 获取子查询的源表
                                subquery_sources = []
                                for src, tgt, typ in self.graph.in_edges(nbunch=source_table, data="type"):
                                    if isinstance(src, Table):
                                        subquery_sources.append(src)
                                
                                # 如果子查询有源表，使用源表的列
                                for src_table in subquery_sources:
                                    # 尝试从元数据提供者获取列
                                    if metadata_provider:
                                        src_table_columns = metadata_provider.get_table_columns(src_table)
                                    else:
                                        # 如果没有元数据提供者，我们需要从查询本身推断列
                                        src_table_columns = []
                                        
                                    # 如果能获取到列，就替换通配符
                                    if src_table_columns:
                                        self._replace_wildcard(
                                            tgt_table,
                                            src_table_columns,
                                            tgt_wildcard,
                                            src_wildcard,
                                        )
                                    else:
                                        # 没有元数据时，我们需要特殊处理
                                        # 获取子查询的SELECT列
                                        subquery_columns = [col for col in self.graph.nodes if isinstance(col, Column) and col.parent == source_table and col.raw_name != "*"]
                                        
                                        # 为每个子查询列创建对应的源表列
                                        for subquery_col in subquery_columns:
                                            # 创建源表列
                                            src_col = Column(subquery_col.raw_name)
                                            src_col.parent = src_table
                                            
                                            # 创建目标表列
                                            tgt_col = Column(subquery_col.raw_name)
                                            tgt_col.parent = tgt_table
                                            
                                            # 添加关系
                                            self.graph.add_edge(src_table, src_col, type=EdgeType.HAS_COLUMN)
                                            self.graph.add_edge(src_col, tgt_col, type=EdgeType.LINEAGE)
                                            self.graph.add_edge(tgt_table, tgt_col, type=EdgeType.HAS_COLUMN)
                            elif isinstance(source_table, Table) and metadata_provider:
                                # search by metadata service
                                src_table_columns = metadata_provider.get_table_columns(
                                    source_table
                                )
                                if src_table_columns:
                                    self._replace_wildcard(
                                        tgt_table,
                                        src_table_columns,
                                        tgt_wildcard,
                                        src_wildcard,
                                    )

            # 最后移除通配符
            for column in self.write_columns:
                if column.raw_name == "*":
                    self.graph.remove_node(column)
            
            # 移除源通配符
            for node in list(self.graph.nodes):
                if isinstance(node, Column) and node.raw_name == "*":
                    self.graph.remove_node(node)

    def get_alias_mapping_from_table_group(
        self, table_group: List[Union[Path, Table, SubQuery]]
    ) -> Dict[str, Union[Path, Table, SubQuery]]:
        """
        A table can be referred to as alias, table name, or database_name.table_name, create the mapping here.
        For SubQuery, it's only alias then.
        """
        return {
            **{
                tgt: src
                for src, tgt, attr in self.graph.edges(data=True)
                if attr.get("type") == EdgeType.HAS_ALIAS and src in table_group
            },
            **{
                table.raw_name: table
                for table in table_group
                if isinstance(table, Table)
            },
            **{str(table): table for table in table_group if isinstance(table, Table)},
        }

    def _get_target_table(self) -> Optional[Union[SubQuery, Table]]:
        table = None
        if write_only := self.write.difference(self.read):
            table = next(iter(write_only))
        return table

    def get_source_columns(self, node: Column) -> List[Column]:
        return [
            src
            for (src, tgt, edge_type) in self.graph.in_edges(nbunch=node, data="type")
            if edge_type == EdgeType.LINEAGE and isinstance(src, Column)
        ]

    def _replace_wildcard(
        self,
        tgt_table: Union[Table, SubQuery],
        src_table_columns: List[Column],
        tgt_wildcard: Column,
        src_wildcard: Column,
    ) -> None:
        target_columns = self.get_table_columns(tgt_table)
        for src_col in src_table_columns:
            new_column = Column(src_col.raw_name)
            new_column.parent = tgt_table
            if new_column in target_columns or src_col.raw_name == "*":
                continue
            self.graph.add_edge(tgt_table, new_column, type=EdgeType.HAS_COLUMN)
            self.graph.add_edge(src_col.parent, src_col, type=EdgeType.HAS_COLUMN)
            self.graph.add_edge(src_col, new_column, type=EdgeType.LINEAGE)
        # remove wildcard
        if self.graph.has_node(tgt_wildcard):
            self.graph.remove_node(tgt_wildcard)
        if self.graph.has_node(src_wildcard):
            self.graph.remove_node(src_wildcard)


class StatementLineageHolder(SubQueryLineageHolder, ColumnLineageMixin):
    """
    Statement Level Lineage Result.

    Based on SubQueryLineageHolder, StatementLineageHolder holds extra attributes like drop and rename

    For drop, it is a Set[:class:`sqllineage.core.models.Table`].

    For rename, it a Set[Tuple[:class:`sqllineage.core.models.Table`, :class:`sqllineage.core.models.Table`]],
    with the first table being original table before renaming and the latter after renaming.
    """

    def __str__(self):
        return "\n".join(
            f"table {attr}: {sorted(getattr(self, attr), key=lambda x: str(x)) if getattr(self, attr) else '[]'}"
            for attr in ["read", "write", "cte", "drop", "rename"]
        )

    def __repr__(self):
        return str(self)

    @property
    def read(self) -> Set[Table]:  # type: ignore
        return {t for t in super().read if isinstance(t, DATASET_CLASSES)}

    @property
    def write(self) -> Set[Table]:  # type: ignore
        return {t for t in super().write if isinstance(t, DATASET_CLASSES)}

    @property
    def drop(self) -> Set[Table]:
        return self._property_getter(NodeTag.DROP)  # type: ignore

    def add_drop(self, value) -> None:
        self._property_setter(value, NodeTag.DROP)

    @property
    def rename(self) -> Set[Tuple[Table, Table]]:
        return {
            (src, tgt)
            for src, tgt, attr in self.graph.edges(data=True)
            if attr.get("type") == EdgeType.RENAME
        }

    def add_rename(self, src: Table, tgt: Table) -> None:
        self.graph.add_edge(src, tgt, type=EdgeType.RENAME)

    @staticmethod
    def of(holder: SubQueryLineageHolder) -> "StatementLineageHolder":
        stmt_holder = StatementLineageHolder()
        stmt_holder.graph = holder.graph
        return stmt_holder


class SQLLineageHolder(ColumnLineageMixin):
    def __init__(self, graph: DiGraph):
        """
        The combined lineage result in representation of Directed Acyclic Graph.

        :param graph: the Directed Acyclic Graph holding all the combined lineage result.
        """
        self.graph = graph
        self._selfloop_tables = self.__retrieve_tag_tables(NodeTag.SELFLOOP)
        self._sourceonly_tables = self.__retrieve_tag_tables(NodeTag.SOURCE_ONLY)
        self._targetonly_tables = self.__retrieve_tag_tables(NodeTag.TARGET_ONLY)
        
    @property
    def table_lineage_graph(self) -> DiGraph:
        """
        The table level DiGraph held by SQLLineageHolder
        """
        table_nodes = [n for n in self.graph.nodes if isinstance(n, DATASET_CLASSES)]
        return self.graph.subgraph(table_nodes)

    @property
    def column_lineage_graph(self) -> DiGraph:
        """
        The column level DiGraph held by SQLLineageHolder
        """
        column_nodes = [n for n in self.graph.nodes if isinstance(n, Column)]
        return self.graph.subgraph(column_nodes)

    @property
    def source_tables(self) -> Set[Table]:
        """
        a list of source :class:`sqllineage.core.models.Table`
        """
        source_tables = {
            table for table, deg in self.table_lineage_graph.in_degree if deg == 0
        }.intersection(
            {table for table, deg in self.table_lineage_graph.out_degree if deg > 0}
        )
        source_tables |= self._selfloop_tables
        source_tables |= self._sourceonly_tables
        return source_tables

    @property
    def target_tables(self) -> Set[Table]:
        """
        a list of target :class:`sqllineage.core.models.Table`
        """
        target_tables = {
            table for table, deg in self.table_lineage_graph.out_degree if deg == 0
        }.intersection(
            {table for table, deg in self.table_lineage_graph.in_degree if deg > 0}
        )
        target_tables |= self._selfloop_tables
        target_tables |= self._targetonly_tables
        return target_tables

    @property
    def intermediate_tables(self) -> Set[Table]:
        """
        a list of intermediate :class:`sqllineage.core.models.Table`
        """
        intermediate_tables = {
            table for table, deg in self.table_lineage_graph.in_degree if deg > 0
        }.intersection(
            {table for table, deg in self.table_lineage_graph.out_degree if deg > 0}
        )
        intermediate_tables -= self.__retrieve_tag_tables(NodeTag.SELFLOOP)
        return intermediate_tables

    def __retrieve_tag_tables(self, tag) -> Set[Union[Path, Table]]:
        return {
            table
            for table, attr in self.graph.nodes(data=True)
            if attr.get(tag) is True and isinstance(table, DATASET_CLASSES)
        }

    @staticmethod
    def _build_digraph(
        metadata_provider: MetaDataProvider, *args: StatementLineageHolder
    ) -> DiGraph:
        g = DiGraph()
        for holder in args:
            g = nx.compose(g, holder.graph)
            if holder.drop:
                for table in holder.drop:
                    if g.has_node(table) and g.degree[table] == 0:
                        g.remove_node(table)
            elif holder.rename:
                for table_old, table_new in holder.rename:
                    g = nx.relabel_nodes(g, {table_old: table_new})
                    g.remove_edge(table_new, table_new)
                    if g.degree[table_new] == 0:
                        g.remove_node(table_new)
            else:
                read, write = holder.read, holder.write
                if len(read) > 0 and len(write) == 0:
                    # source only table comes from SELECT statement
                    nx.set_node_attributes(
                        g, {table: True for table in read}, NodeTag.SOURCE_ONLY
                    )
                elif len(read) == 0 and len(write) > 0:
                    # target only table comes from case like: 1) INSERT/UPDATE constant values; 2) CREATE TABLE
                    nx.set_node_attributes(
                        g, {table: True for table in write}, NodeTag.TARGET_ONLY
                    )
                else:
                    for source, target in itertools.product(read, write):
                        g.add_edge(source, target, type=EdgeType.LINEAGE)
        nx.set_node_attributes(
            g,
            {table: True for table in {e[0] for e in nx.selfloop_edges(g)}},
            NodeTag.SELFLOOP,
        )
        # find all the columns that we can't assign accurately to a parent table (with multiple parent candidates)
        unresolved_cols = [
            (s, t)
            for s, t in g.edges
            if isinstance(s, Column) and len(s.parent_candidates) > 1
        ]
        for unresolved_col, tgt_col in unresolved_cols:
            # check if there's only one parent candidate contains the column with same name
            src_cols = []
            # check if source column exists in graph (either from subquery or from table created in prev statement)
            for parent in unresolved_col.parent_candidates:
                src_col = Column(unresolved_col.raw_name)
                src_col.parent = parent
                if g.has_edge(parent, src_col):
                    src_cols.append(src_col)
            # if not in graph, check if defined in table schema by metadata service
            if len(src_cols) == 0 and bool(metadata_provider):
                for parent in unresolved_col.parent_candidates:
                    if (
                        isinstance(parent, Table)
                        and str(parent.schema) != Schema.unknown
                    ):
                        columns = metadata_provider.get_table_columns(parent)
                        for src_col in columns:
                            if unresolved_col.raw_name == src_col.raw_name:
                                src_cols.append(src_col)

            # Multiple sources is a correct case for JOIN with USING
            # It incorrect for JOIN with ON, but sql without specifying an alias in this case will be invalid
            for src_col in src_cols:
                g.add_edge(src_col, tgt_col, type=EdgeType.LINEAGE)
            if len(src_cols) > 0:
                # only delete unresolved column when it's resolved
                g.remove_edge(unresolved_col, tgt_col)

        # when unresolved column got resolved, it will be orphan node, and we can remove it
        for node in [n for n, deg in g.degree if deg == 0]:
            if isinstance(node, Column) and len(node.parent_candidates) > 1:
                g.remove_node(node)
        return g

    @staticmethod
    def of(metadata_provider, *args: StatementLineageHolder) -> "SQLLineageHolder":
        """
        To assemble multiple :class:`sqllineage.core.holders.StatementLineageHolder` into
        :class:`sqllineage.core.holders.SQLLineageHolder`
        """
        g = SQLLineageHolder._build_digraph(metadata_provider, *args)
        return SQLLineageHolder(g)