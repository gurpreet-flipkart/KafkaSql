package org.kafka.grep.translation;

import org.kafka.grep.transformer.SqlTransformer;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

public class ClauseBuilder<T> {
    private Class<? extends Accumulator<T>> accumulatorClass;
    private SqlTransformer<T> transformer;

    public ClauseBuilder(Class<? extends Accumulator<T>> accumulatorClass, SqlTransformer<T> transformer) {
        this.accumulatorClass = accumulatorClass;
        this.transformer = transformer;
    }

    public T build(SqlNode node) {
        Accumulator<T> a = null;
        try {
            a = accumulatorClass.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(node);
        if (node instanceof SqlBasicCall) {
            SqlBasicCall sbCall = (SqlBasicCall) node;
            if (node.getKind() == SqlKind.AND) {
                for (SqlNode operand : sbCall.getOperandList()) {
                    a.mergeAND(build(operand));
                }
            } else if (node.getKind() == SqlKind.OR) {
                for (SqlNode n : sbCall.getOperandList()) {
                    a.mergeOR(build(n));
                }
            } else {
                System.out.println(node.getKind());
                a.mergeCondition(transformer.transform(sbCall.getOperator(), sbCall.getOperandList()));
            }
        }
        return a.get();
    }
}
