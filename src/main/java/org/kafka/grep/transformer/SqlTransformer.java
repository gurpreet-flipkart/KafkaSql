package org.kafka.grep.transformer;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

public interface SqlTransformer<T> {

    T transform(SqlOperator operator, List<SqlNode> operands);
}

