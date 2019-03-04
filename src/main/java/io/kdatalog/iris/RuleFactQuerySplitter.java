package io.kdatalog.iris;

import org.deri.iris.api.basics.IPredicate;
import org.deri.iris.api.basics.IQuery;
import org.deri.iris.api.basics.IRule;
import org.deri.iris.compiler.Parser;
import org.deri.iris.storage.IRelation;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

public class RuleFactQuerySplitter {
    private Map<IPredicate, IRelation> facts;
    private List<IRule> rules;
    private List<IQuery> queries;

    public RuleFactQuerySplitter(String program) {
        this(new StringReader(program));
    }

    public RuleFactQuerySplitter(Reader program) {
        try {
            Parser parser = new Parser();
            parser.parse(program);
            facts = parser.getFacts();
            rules = parser.getRules();
            queries = parser.getQueries();
        } catch (Exception e) {
            throw new RuntimeException("Could not parse program: " + program, e);
        }
    }

    public Map<IPredicate, IRelation> getFacts() {
        return facts;
    }

    public List<IRule> getRules() {
        return rules;
    }

    public List<IQuery> getQueries() {
        return queries;
    }
}
