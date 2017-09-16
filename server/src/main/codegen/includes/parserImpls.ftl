<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = Lists.newArrayList();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void TableElement(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final boolean virtual;
}
{
    id = SimpleIdentifier()
    (
        type = DataType()
        (
            <NULL> { nullable = true; }
        |
            <NOT> <NULL> { nullable = false; }
        |
            { nullable = true; }
        )
        (
            [ <GENERATED> <ALWAYS> ] <AS> <LPAREN>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
            (
                <VIRTUAL> { virtual = true; }
            |
                <STORED> { virtual = false; }
            |
                { virtual = true; }
            )
        |
            <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) { virtual = false; }
        |
            { e = null; virtual = false; }
        )
        {
            list.add(
                SqlDdlNodes.column(s.add(id).end(this), id,
                    type.withNullable(nullable), e, virtual));
        }
    |
        { list.add(id); }
    )
|
    id = SimpleIdentifier() {
        list.add(id);
    }
|
    [ <CONSTRAINT> { s.add(this); } name = SimpleIdentifier() ]
    (
        <CHECK> { s.add(this); } <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN> {
            list.add(SqlDdlNodes.check(s.end(this), name, e));
        }
    |
        <UNIQUE> { s.add(this); }
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
        }
    |
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    )
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode query = null;
}
{
    <TABLE> id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    [ <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ]
    {
        return SqlDdlNodes.createTable(s.end(this), id, tableElementList, query);
    }
}

SqlCreate SqlCreateView(Span s, boolean replace) :
{
    boolean materialized = false;
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    [ <MATERIALIZED>  { materialized = true; } ]
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createView(s.end(this), materialized, replace, id,
            columnList, query);
    }
}

// End parserImpls.ftl
