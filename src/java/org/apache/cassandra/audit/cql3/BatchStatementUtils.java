/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.audit.cql3;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.Token;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CqlLexer;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.PreparedStatementCache;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.MD5Digest;

public class BatchStatementUtils
{
    private static final Logger logger = LoggerFactory.getLogger(BatchStatementUtils.class);

    public static class Meta
    {
        public final String query;
        public final int varsOffset;
        public final int varsSize;

        public Meta(String query, int varsOffset, int varsSize)
        {
            this.query = query;
            this.varsOffset = varsOffset;
            this.varsSize = varsSize;
        }

        public <T> List<T> getSubList(List<T> l)
        {
            return l.subList(varsOffset, varsOffset + varsSize);
        }
    }

    /**
     * For audit logging we need to decompose the string version of a
     * CQL batch statement into component statements. Understandably,
     * there's no way to extract a CQL string from a parsed statement
     * (it could be done but would require a lot of mods to the statement
     * classes), so here we re-use the CQL3 lexer to deconstruct the
     * original batch statement into a number of individual statement
     * strings, and determine which bound variables each references.
     */
    public static List<Meta> decomposeBatchStatement(String queryStr)
    {
        CharStream stream = new ANTLRStringStream(queryStr);
        CqlLexer lexer = new CqlLexer(stream);
        boolean pastProlog = false;
        List<Meta> stmts = new LinkedList<>();
        StringBuilder builder = new StringBuilder();
        int numVars = 0;
        int varsOffset = 0;


        for (Token t = lexer.nextToken(); t.getType() != Token.EOF; t = lexer.nextToken())
        {
            // skip purely whitespace tokens
            if (t.getType() == CqlParser.WS)
            {
                continue;
            }

            // don't start collecting statement until we're past
            // the "BEGIN BATCH....."
            if (isStatementStart(t))
            {
                if (pastProlog)
                {
                    // remove the whitespace the lexer added into ks.cf strings
                    String query = builder.toString().trim().replaceAll(" \\. ", "\\.");
                    stmts.add(new Meta(query, varsOffset, numVars - varsOffset));
                    builder = new StringBuilder();
                }
                pastProlog = true;
                varsOffset = numVars;
            }

            if (pastProlog)
            {
                // once we get to the "APPLY" in "APPLY BATCH" we're done
                if (t.getType() == CqlParser.K_APPLY)
                {
                    // remove the whitespace the lexer added into ks.cf strings
                    String query = builder.toString().trim().replaceAll(" \\. ", "\\.");
                    stmts.add(new Meta(query, varsOffset, numVars - varsOffset));
                    break;
                }

                // re-wrap String literals in single quotes
                if (t.getType() == CqlParser.STRING_LITERAL)
                {
                    builder.append('\'').append(t.getText()).append('\'').append(' ');
                }
                else
                {
                    builder.append(t.getText()).append(' ');
                }
            }

            // count bound variables for each statement
            if (t.getType() == CqlParser.QMARK)
            {
                numVars++;
            }
        }
        return stmts;
    }

    public static List<String> decomposeBatchStatement(QueryState queryState, BatchQueryOptions queryOptions)
    {
        List<String> cqlStrings = new ArrayList<>();
        List<Object> queryOrIdList = queryOptions.getQueryOrIdList();

        for (Object queryOrId : queryOrIdList)
        {
            if (queryOrId instanceof String)
            {
                cqlStrings.add((String) queryOrId);
            }
            else if (queryOrId instanceof MD5Digest)
            {
                // prepared statement
                // lookup the original prepared stmt from QP's cache
                // then use it as the key to fetch CQL string & column
                // specs for bind vars from our own cache
                CQLStatement prepared = ClientState.getCQLQueryHandler()
                    .getPrepared((MD5Digest) queryOrId).statement;
                cqlStrings.add(PreparedStatementCache.instance.getQueryInfo(prepared).left);
            }
        }
        return cqlStrings;
    }

    private static boolean isStatementStart(Token t)
    {
        switch (t.getType())
        {
            case CqlParser.K_INSERT:
            case CqlParser.K_UPDATE:
            case CqlParser.K_DELETE:
                return true;
            default:
                return false;
        }
    }

    
}
