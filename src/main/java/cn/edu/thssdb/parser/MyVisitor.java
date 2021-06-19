package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.StatementException;
import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.query.Conditions;
import cn.edu.thssdb.query.Expression;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.mNumber;
import javafx.util.Pair;
import cn.edu.thssdb.query.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedList;

public class MyVisitor extends SQLBaseVisitor {
    public MyVisitor() {
        super();
    }

    @Override
    public Object visitParse(SQLParser.ParseContext ctx) {
        return visit(ctx.getChild(0));
    }

    @Override
    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        int n = ctx.getChildCount();
        ArrayList res = new ArrayList();
        for (int ctx_index = 0; ctx_index < n; ctx_index++) {
            //跳过分号
            if (!ctx.getChild(ctx_index).getText().equals(";")) {
                res.add(visit(ctx.getChild(ctx_index)));
            }
        }
        return res;
    }

    @Override
    public Object visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        return visit(ctx.getChild(0));
    }

    @Override
    public Object visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        String dbname = (String) visit(ctx.getChild(2));
        return new StatementDatabase(dbname, 1);
    }

    @Override
    public Object visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        String dbname = (String) visit(ctx.getChild(1));
        return new StatementDatabase(dbname, 2);
    }

    @Override
    public Object visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        String dbname;
        //含有if exists
        if(ctx.getChildCount() == 5) {
            dbname = (String) visit(ctx.getChild(4));
        }
        else {
            dbname = (String) visit(ctx.getChild(2));
        }
        return new StatementDatabase(dbname, 3);
    }

    @Override
    public Object visitDatabase_name(SQLParser.Database_nameContext ctx) {
        return (String) ctx.getChild(0).toString().toUpperCase();
    }

    @Override
    public Object visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String tableName = (String) visit(ctx.getChild(2));
        int n = ctx.getChildCount();
        ArrayList<Column> cols = new ArrayList<>();
        //顺次往后遍历每个孩子节点
        for(int i = 4; i < n; i++) {
            //这是create table语句的末尾
            if (ctx.getChild(i).getText().toUpperCase().contains("PRIMARY")) {
                String colName = ctx.getChild(i).getChild(3).getText().toUpperCase();
                for (Column col : cols) {
                    if (col.getName().equals(colName)) {
                        col.setPrimary(1);
                    }
                }
                break;
            }

            //读出一个列的信息
            ArrayList col_def = (ArrayList) visit(ctx.getChild(i));
            String colName = (String) col_def.get(0);
            String typeInfo = (String) col_def.get(1);
            ColumnType colType;
            int maxLength;
            //把类型和长度解析出来
            if (typeInfo.contains("(")) {
                int startPos = typeInfo.indexOf('(');
                int endPos = typeInfo.indexOf(')');
                colType = ColumnType.fromStrToType(typeInfo.substring(0, startPos));
                maxLength = Integer.parseInt(typeInfo.substring(startPos + 1, endPos));
            }
            //如果没有指定长度：字符串因为必须指定长度，所以抛出异常。其他类型给默认指定为最大长度
            else {
                colType = ColumnType.fromStrToType(typeInfo);
                if (colType == ColumnType.STRING)
                    throw new StatementException(StatementException.LACK_OF_MAXLENGTH_ERROR);
                else {
                    maxLength = mNumber.byteOfType(colType);
                }
            }
            boolean notNull = false;
            if(col_def.size() >= 3)
                notNull = (boolean) col_def.get(2);

            //用上面解析到的信息新建一列
            cols.add(new Column(colName, colType, 0, notNull, maxLength));
            //往下移一个，跳过','
            i += 1;
        }

        Column[] columns = new Column[cols.size()];
        for(int i = 0; i < cols.size(); i++) {
            columns[i] = cols.get(i);
        }
        return new StatementCreateTable(tableName, columns);
//        return null;
    }

    @Override
    public Object visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tableName = ctx.getChild(2).getText().toUpperCase();
        return new StatementDropTable(tableName);
//        return null;
    }

    @Override
    public Object visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        String tableName = ctx.getChild(2).getText().toUpperCase();

        return new StatementShowTable(tableName);
    }

    @Override
    public Object visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = (String) visit(ctx.getChild(2));
        int ctx_index = 3;
        LinkedList<String> colNames = null;
        //含有待插入的列名，把它们解析出来
        if(!ctx.getChild(ctx_index).getText().toUpperCase().equals("VALUES")) {
            colNames = new LinkedList<>();
            ctx_index += 1;
            colNames.add((String) visit(ctx.getChild(ctx_index)));
            //下一个字符不是)，说明后面还有列名
            while(!ctx.getChild(ctx_index+1).getText().toUpperCase().equals(")")) {
                ctx_index += 2;
                colNames.add((String) visit(ctx.getChild(ctx_index)));
            }
            ctx_index += 2;
        }

        //当前ctx_index指向"values_entry"
        ctx_index += 1;
        LinkedList values = new LinkedList<>();
        int token_num = ctx.getChildCount();
        while(true) {
            //应该只有一组values
            values = (LinkedList) visit(ctx.getChild(ctx_index));
            ctx_index += 2;
            if(ctx_index >= token_num)
                break;
        }
        if(colNames == null) {
            return new StatementInsert(tableName, values);
        }
        else {
            return new StatementInsert(tableName, colNames, values);
        }
    }

    @Override
    public Object visitValue_entry(SQLParser.Value_entryContext ctx) {
        int ctx_index = 1;
        LinkedList values = new LinkedList();
        Expression expr = (Expression) visit(ctx.getChild(ctx_index));
        Pair<Object, ColumnType> v;
        try {
            v = expr.getDirectValue();
            values.add(v.getKey());
            while (true) {
                ctx_index += 2;
                if (ctx_index >= ctx.getChildCount())
                    break;
                expr = (Expression) visit(ctx.getChild(ctx_index));
                v = expr.getDirectValue();
                values.add(v.getKey());
            }
        } catch (Exception e) {
            throw new NDException("error while parsing value_entry.");  //change
        }
        return values;
    }

    @Override
    public Object visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String tableName = (String) visit(ctx.getChild(2));
        if(ctx.getChildCount() > 3) {
            Conditions cond = (Conditions) visit(ctx.getChild(4));
            return new StatementDelete(tableName, cond);
        }
        else {
            return new StatementDelete(tableName);
        }
    }

    @Override
    public Object visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        String tableName = (String) visit(ctx.getChild(1));
        LinkedList<String> colList = new LinkedList<>();
        colList.add((String) visit(ctx.getChild(3)));
        LinkedList<Expression> exprList = new LinkedList<>();
        exprList.add((Expression) visit(ctx.getChild(5)));
        //有where条件
        if(ctx.getChildCount() > 6) {
            Conditions cond = (Conditions) visit(ctx.getChild(7));
            return new StatementUpdate(tableName, colList, exprList, cond);
        }
        else
            return new StatementUpdate(tableName, colList, exprList);
    }

    @Override
    public Object visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        int ctx_index = 1;
        //是否指定distinct or all
        boolean use_distinct_or_all = false;
        String condition = ctx.getChild(ctx_index).getText().toUpperCase();
        if(condition.equals("DISTINCT") || condition.equals("ALL")) {
            use_distinct_or_all = true;
            ctx_index += 1;
        }

        //现在ctx_index指向result_column
        LinkedList<String> selected_colNames = new LinkedList<>();
        String colName = (String) visit(ctx.getChild(ctx_index));
        selected_colNames.add(colName);
        //如果后面还有要选择的其它列名
        while(ctx.getChild(ctx_index+1).getText().toUpperCase().equals(",")) {
            ctx_index += 2;
            colName = (String) visit(ctx.getChild(ctx_index));
            selected_colNames.add(colName);
        }

        //现在ctx_index指向最后一个result_column
        ctx_index += 2;
        //开始解析from语句
        RangeVariable rv = (RangeVariable) visit(ctx.getChild(ctx_index));
        //TODO: 会存在下一个table_query吗？

        //如果后面存在where语句：解析where
        if(ctx.getChildCount() > ctx_index+1) {
            ctx_index += 2;
            Conditions cond = (Conditions) visit(ctx.getChild(ctx_index));
            return new StatementSelect(selected_colNames, rv, cond, use_distinct_or_all);
        }
        return new StatementSelect(selected_colNames, rv, null, use_distinct_or_all);

    }

    @Override
    public Object visitResult_column(SQLParser.Result_columnContext ctx) {
        //*
        if(ctx.getText().toUpperCase().equals("*"))
            return "*";
        //tableName.*
        else if(ctx.getChildCount() == 3) {
            String tableName = (String) visit(ctx.getChild(0));
            return tableName+".*";
        }
        //column_full_name
        else if(ctx.getChildCount() == 1) {
            return visit(ctx.getChild(0));
        }
        else
            throw new StatementException(StatementException.SELECT_SYNTAX_ERROR);
    }

    @Override
    public Object visitColumn_full_name(SQLParser.Column_full_nameContext ctx) {
        //tableName.colName
        if(ctx.getChildCount() == 3) {
            String tableName = (String) visit(ctx.getChild(0));
            String colName = (String) visit(ctx.getChild(2));
            return tableName+"."+colName;
        }
        //colName
        else if(ctx.getChildCount() == 1) {
            return visit(ctx.getChild(0));
        }
        else
            throw new StatementException(StatementException.SELECT_SYNTAX_ERROR);
    }

    @Override
    public Object visitTable_query(SQLParser.Table_queryContext ctx) {
        //from的第一个表
        String tableName = (String) visit(ctx.getChild(0));

        //只from一个表
        if(ctx.getChildCount() == 1) {
            return new RangeVariable(tableName, null);
        }
        //含join
        else {
            int token_num = ctx.getChildCount();
            int join_table_num = (token_num+3)/4;   //一共要from的表数

            RangeVariable rv = new RangeVariable(null, null);
            //rvs保存要join的每一张表，和它们之间的关系
            ArrayList<RangeVariable> rvs = new ArrayList<>();
            //第一张表先填进去
            RangeVariable rv_firstTable = new RangeVariable(tableName, null);
            rvs.add(rv_firstTable);
            //顺次添加后面的每一张表
            int ctx_index = 2;
            for(int i = 1; i < join_table_num; i++) {
                tableName = (String) visit(ctx.getChild(ctx_index));
                RangeVariable rv_oneTable = new RangeVariable(tableName, null);
                ctx_index += 2;
                Conditions cond = (Conditions) visit(ctx.getChild(ctx_index));
                rv_oneTable.setConditions(cond);
                ctx_index += 2;
                rvs.add(rv_oneTable);
            }
            rv.setRangeVariables(rvs);
            return rv;
        }
    }

    @Override
    public Object visitCondition(SQLParser.ConditionContext ctx) {
        Expression expr1 = (Expression) visit(ctx.getChild(0));
        String op = (String) visit(ctx.getChild(1));
        Expression expr2 = (Expression) visit(ctx.getChild(2));
        switch (op) {
            case "=":
                return new Conditions(2, "EQ", expr1, expr2);
            case "<>":
                return new Conditions(2, "NEQ", expr1, expr2);
            case "<":
                return new Conditions(2, "LT", expr1, expr2);
            case ">":
                return new Conditions(2, "GT", expr1, expr2);
            case "<=":
                return new Conditions(2, "NGT", expr1, expr2);
            case ">=":
                return new Conditions(2, "NLT", expr1, expr2);
            default:
                throw new StatementException(StatementException.OPERATION_TYPE_ERROR);
        }
    }

    @Override
    public Object visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        //只有一个condition
        if(ctx.getChildCount() == 1) {
            return visit(ctx.getChild(0));
        }
        //and / or
        else {
            Conditions cond1 = (Conditions) visit(ctx.getChild(0));
            Conditions cond2 = (Conditions) visit(ctx.getChild(2));
            String op = ctx.getChild(1).getText().toUpperCase();
            if(op.equals("&&"))
                return new Conditions(0, cond1, cond2);
            else if(op.equals("||"))
                return new Conditions(1, cond1, cond2);
            else
                throw new StatementException(StatementException.LOGICAL_OPERATION_TYPE_ERROR);
        }
    }

    @Override
    public Object visitExpression(SQLParser.ExpressionContext ctx) {
        //comparer
        if(ctx.getChildCount() == 1) {
            return visit(ctx.getChild(0));
        }
        //(expression)
        else if(ctx.getChild(0).getText().equals("(")) {
            return visit(ctx.getChild(1));
        }
        //expression op expression
        else {
            Expression expr1 = (Expression) visit(ctx.getChild(0));
            Expression expr2 = (Expression) visit(ctx.getChild(2));
            String op = ctx.getChild(1).getText();
            return new Expression(3, op, expr1, expr2);
        }

    }

    @Override
    public Object visitComparator(SQLParser.ComparatorContext ctx) {
        return ctx.getText().toUpperCase();
    }

    @Override
    public Object visitComparer(SQLParser.ComparerContext ctx) {
        Object o = visit(ctx.getChild(0));
        //说明解析出来的是column_full_name
        if(o instanceof String) {
            return new Expression(1, ctx.getText().toUpperCase());
        }
        //literal_value
        else {
            return visit(ctx.getChild(0));
        }
    }

    @Override
    public Object visitLiteral_value(SQLParser.Literal_valueContext ctx) {
        //判断是不是字符串，如果是字符串要去掉首尾的引号
        String value = ctx.getText().toUpperCase();
        if(value.startsWith("'")) {
            value = value.substring(1, value.length()-1);
            return new Expression(4, value);
        }
        //TODO: 把null放在String类型里
        else if(value.equals("NULL")) {
            return new Expression(4, value);
        }
        //数字
        else {
            return new Expression(0, ctx.getText());
        }
    }


    @Override
    public Object visitTable_name(SQLParser.Table_nameContext ctx) {
        return ctx.getText().toUpperCase();
    }

    @Override
    public Object visitColumn_def(SQLParser.Column_defContext ctx) {
        ArrayList col_def = new ArrayList();
        //分别是列名、类型
        for(int i = 0; i < ctx.getChildCount(); i++) {
            col_def.add(visit(ctx.getChild(i)));
        }
        return col_def;
    }

    //col_def的三个属性
    @Override
    public Object visitColumn_name(SQLParser.Column_nameContext ctx) {
        return ctx.getText().toUpperCase();
    }

    @Override
    public Object visitType_name(SQLParser.Type_nameContext ctx) {
        return ctx.getText().toUpperCase();
    }

    @Override
    public Object visitColumn_constraint(SQLParser.Column_constraintContext ctx) {
        if (ctx.getChild(0).getText().toUpperCase().compareTo("NOT") == 0)
            return true;
        //TODO: 会有其他情况吗？
        else
            return false;
    }

    //transaction

    @Override
    public Object visitTransac_stmt(SQLParser.Transac_stmtContext ctx) {
        String trans = (String) visit(ctx.getChild(0));
        if(trans.equals("BEGIN TRANSACTION")) {
            return new StatementTransaction(1);
        }
        //TODO: 如何返回？
        return new StatementTransaction(2);
    }

    @Override
    public Object visitBegin_transaction(SQLParser.Begin_transactionContext ctx) {
        return "BEGIN TRANSACTION";
    }

    @Override
    public Object visitEnd_transaction(SQLParser.End_transactionContext ctx) {
        return "COMMIT";
    }
}
