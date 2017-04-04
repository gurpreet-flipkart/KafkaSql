package org.kafka.grep.grepper;

import org.kafka.grep.translation.Measure;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SQLParser {
    //String query = "select count(a.foo), * from foobar where foo.bar='foobar' and (a.b.c.e=50 or x=20) and y=12 group by bag, shipment";

    public List<String> selectors(SqlSelect node) {
        return node.getSelectList().getList().stream().map(x -> x.toString()).collect(Collectors.toList());

    }

    public boolean distinct(SqlSelect node) {
        SqlNode distinct = node.getModifierNode(SqlSelectKeyword.DISTINCT);
        return distinct != null;
    }

    public String[] from(SqlSelect node) {
        SqlIdentifier from = (SqlIdentifier) (node.getFrom());
        if (from.isSimple()) {
            return new String[]{from.getSimple()};
        } else {
            //assumption is 2
            return new String[]{from.getComponent(0).getSimple(), from.getComponent(1).getSimple()};
        }
    }

    public Optional<SqlSelect> getSqlSelect(String sqlQuery) throws SqlParseException {
        SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder().setQuoting(Quoting.DOUBLE_QUOTE).setCaseSensitive(true).setQuotedCasing(Casing.UNCHANGED).setLex(Lex.MYSQL);
        SqlParser parser = SqlParser.create(sqlQuery, configBuilder.build());
        SqlNode node = parser.parseQuery();
        if (node.getKind() != SqlKind.SELECT) {
            System.err.println("Expected a select query");
            return Optional.empty();
        }
        return Optional.of((SqlSelect) node);
    }

    public List<String> group_by(SqlSelect sqlSelect) {
        return sqlSelect.getGroup().getList().stream().map(n -> n.toString()).collect(Collectors.toList());
    }

    public List<Measure> aggregates(SqlSelect sqlSelect) {
        return sqlSelect.getSelectList().getList().stream().map(n -> (SqlBasicCall) n).map(sbc -> new Measure(sbc.getOperandList().stream().findFirst().get().toString(), sbc.getOperator().toString())).collect(Collectors.toList());
    }
}
