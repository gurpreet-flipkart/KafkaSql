package org.kafka.grep.transformer;

import org.kafka.grep.grepper.PathFinder;
import org.kafka.grep.translation.ClauseBuilder;
import org.kafka.grep.translation.PredicateAccumulator;
import org.apache.calcite.sql.*;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class PredicateTransformer implements SqlTransformer<Predicate> {
    private static BiFunction<String, String, Boolean> equalsComp = (left, right) -> {
        if (left == null || right == null) {
            return false;
        }
        right = unquote(right);
        return String.valueOf(left).equals(right);
    };


    class LikeComparator implements BiFunction<String, String, Boolean> {
        private final Pattern pattern;

        public LikeComparator(String regex) {
            regex = unquote(regex);
            pattern = Pattern.compile(regex);
        }

        @Override
        public Boolean apply(String left, String right) {
            if (left == null || right == null) {
                return false;
            }
            left = unquote(left);
            return pattern.matcher(left).matches();
        }
    }

    private static String unquote(String text) {
        if (text.charAt(0) == '\'' && text.charAt(text.length() - 1) == '\'') {
            return text.substring(1, text.lastIndexOf('\''));
        }
        return text;
    }


    private BiFunction<String, String, Boolean> getSqlTranformer(SqlKind sqlKind) {
        BiFunction<String, String, Boolean> comparator = null;
        switch (sqlKind) {
            case EQUALS:
                comparator = equalsComp;
                break;
            case NOT_EQUALS:
                comparator = (left, right) -> !equalsComp.apply(left, right);
                break;
            case IS_NULL:
                comparator = (left, right) -> left == "null";
                break;
            case IS_NOT_NULL:
                comparator = (left, right) -> left != "null";
                break;
            case LIKE:
                break;
        }
        return comparator;
    }

    private Predicate build(SqlNode node) {
        return new ClauseBuilder<>(PredicateAccumulator.class, this).build(node);
    }


    public Predicate conditionalClause(SqlSelect selectNode) {
        SqlNode whereClause = selectNode.getWhere();
        return build(whereClause);
    }

    @Override
    public Predicate transform(SqlOperator operator, List<SqlNode> operands) {
        BiFunction<String, String, Boolean> comparator = getSqlTranformer(operator.getKind());
        String path = operands.get(0).toString();
        if (operator.getKind() == SqlKind.IS_NULL) {
            return new PathFinder().literalMatchPredicate(path, null, comparator);
        }

        if (operator.getKind() == SqlKind.IS_NOT_NULL) {
            return new PathFinder().literalMatchPredicate(path, null, comparator);
        }
        SqlNode sqlNode = operands.get(1);
        String value = sqlNode.toString();
        if (operator.getKind() == SqlKind.LIKE && sqlNode instanceof SqlLiteral) {
            return new PathFinder().literalMatchPredicate(path, value, new LikeComparator(value));
        }
        System.out.println(path + "~" + value);
        if (sqlNode instanceof SqlLiteral) {
            System.out.println("treating as literal");
            return new PathFinder().literalMatchPredicate(path, value, comparator);
        }
        System.out.println("Treating as identifier");
        return new PathFinder().identifierMatchPredicate(path, value, comparator);
    }
}
