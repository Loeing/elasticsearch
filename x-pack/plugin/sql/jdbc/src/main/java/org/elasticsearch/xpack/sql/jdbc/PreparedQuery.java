/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

class PreparedQuery {

    static class ParamInfo {
        EsType type;
        Object value;

        ParamInfo(Object value, EsType type) {
            this.value = value;
            this.type = type;
        }
    }

    private final String sql;
    private final ParamInfo[] params;

    private PreparedQuery(String sql, int paramCount) {
        this.sql = tableauWorkaround(sql);
        this.params = new ParamInfo[paramCount];
        clearParams();
    }
    
    /*
     * Tableau seems to generate it's queries intended for ES by calling ""."indexName" (beacause it's trying the DB name)
     * this causes a extrenuous input '.' error message. This workaround is intended to fix that error 
     * by removing the offending substring
     * and yes, here too, just to be safe
     */
    private String tableauWorkaround(String sql) {
        return sql.replaceAll("\"\".", "");
    }

    ParamInfo getParam(int param) throws JdbcSQLException {
        if (param < 1 || param > params.length) {
            throw new JdbcSQLException("Invalid parameter index [" + param + "]");
        }
        return params[param - 1];
    }

    void setParam(int param, Object value, EsType type) throws JdbcSQLException {
        if (param < 1 || param > params.length) {
            throw new JdbcSQLException("Invalid parameter index [" + param + "]");
        }
        params[param - 1].value = value;
        params[param - 1].type = type;
    }

    int paramCount() {
        return params.length;
    }

    void clearParams() {
        for (int i = 0; i < params.length; i++) {
            params[i] = new ParamInfo(null, EsType.KEYWORD);
        }
    }

    /**
     * Returns the sql statement
     */
    String sql() {
        return sql;
    }

    /**
     * Returns the parameters if the SQL statement is parametrized
     */
    List<SqlTypedParamValue> params() {
        return Arrays.stream(this.params).map(
                paramInfo -> new SqlTypedParamValue(paramInfo.type.name(), paramInfo.value)
        ).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return sql() + " " + params();
    }

    // Creates a PreparedQuery
    static PreparedQuery prepare(String sql) throws SQLException {
        return new PreparedQuery(sql, SqlQueryParameterAnalyzer.parametersCount(sql));
    }
}