package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.GrammarException;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;


import javafx.util.Pair;

import java.util.ArrayList;
import java.util.LinkedList;

public class Conditions {
    private int type;
    private String op;
    private Conditions leftCond;
    private Conditions rightCond;
    private Expression expr1, expr2;

    /*
        type |         0         |         1       |         2
             |  cond1 and cond2  | cond1 or cond2  |  expr1 op expr2
     */
    // for type 0 or 1
    public Conditions(int type, Conditions leftCond, Conditions rightCond)
            throws GrammarException {
        if (type != 0 && type != 1) {
            throw new GrammarException("Exception: it is not a basic type for conditions");
        }
        this.type = type;
        this.leftCond = leftCond;
        this.rightCond = rightCond;
    }

    // for type 2
    public Conditions(int type, String op, Expression expr1, Expression expr2)
            throws GrammarException {
        if (type != 2) {
            throw new GrammarException("Exception: it is not a basic type for conditions");
        }
        this.type = type;
        this.expr1 = expr1;
        this.expr2 = expr2;
        this.op = op;
    }

    /*
        test if this condition is satisfied
        params:
            nameList:   symbols' name
            typeList:   symbols' types
            valueList:  symbols' values
        return:
            true if satisfied, otherwise false
     */
    public boolean satisfied(LinkedList<String> nameList, LinkedList<ColumnType> typeList, LinkedList valueList) {
        switch (this.type) {
            case 0:
                return leftCond.satisfied(nameList, typeList, valueList) && rightCond.satisfied(nameList, typeList, valueList);
            case 1:
                return leftCond.satisfied(nameList, typeList, valueList) || rightCond.satisfied(nameList, typeList, valueList);
            case 2:
                Pair<Object, ColumnType> tmp1 = expr1.calcValue(nameList, typeList, valueList);
                Pair<Object, ColumnType> tmp2 = expr2.calcValue(nameList, typeList, valueList);
                ColumnType finalType = ColumnType.lift(tmp1.getValue(), tmp2.getValue());
                try {
                    if (finalType == ColumnType.STRING) {
                        return check(tmp1.getKey(), tmp2.getKey(), this.op, finalType);
                    }
                    Object obj1 = ColumnType.convert(tmp1.getKey(), finalType);
                    Object obj2 = ColumnType.convert(tmp2.getKey(), finalType);
                    return check(obj1, obj2, this.op, finalType);
                } catch (ClassCastException e)
                {
                    throw new GrammarException(e.getMessage());
                }
            default:
                throw new GrammarException("Unexpected Conditions type!");
        }
    }

    /*
        convert column name to table_name.column_name
        params: table list
        return: none
     */
    public void normalize(ArrayList<Table> tables, LinkedList<String> ignoreNames) {
        switch (this.type) {
            case 0:
            case 1:
                leftCond.normalize(tables, ignoreNames);
                rightCond.normalize(tables, ignoreNames);
                break;
            case 2:
                expr1.normalize(tables, ignoreNames);
                expr2.normalize(tables, ignoreNames);
                break;
        }
    }

    public void normalize(ArrayList<Table> tables){
        normalize(tables, null);
    }

    /*
         get the value in "symbol = value"
         params:
            none
        return:
            one Pair<> value p, p's first value is real numeric/string, p's second value is its type
         * Note: You must call isSymbolEqualSomething(symbol) to check the form
     */
    public Pair<Object, ColumnType> getEqualValue(){
        if (expr1.isValue())
            return expr1.getDirectValue();
        return expr2.getDirectValue();
    }

    /*
         get the x in "(-∞, x)" or "(-∞, x]" or "(x, +∞)" or "[x, +∞)"
         params:
            none
        return:
            one Pair<> value p, p's first value is real numeric and its type, p's second value is True if open, else closed
         * Note: You must call isUpperBounded(symbol) or isLowerBound(symbol) to check the form
     */
    public Pair<Pair<Object, ColumnType>, Boolean> getBoundValue(){
        Pair<Object, ColumnType> obj;
        if (expr1.isValue()) {
            obj = expr1.getDirectValue();
        } else
        {
            obj = expr2.getDirectValue();
        }
        if (op.compareTo("LT") == 0 || op.compareTo("GT") == 0)
            return new Pair<>(obj, true);
        return new Pair<>(obj, false);
    }

    /*
         get the x, y in "(/[x, y)/]"
         params:
            none
        return:
            one Pair<> value p, p's first value is left real numeric and its type and openness,
                                p's second value is right real numeric and its type and openness
         * Note: You must call isRanged(symbol) to check the form
     */
    public Pair<Pair<Pair<Object, ColumnType>, Boolean>, Pair<Pair<Object, ColumnType>, Boolean>> getRange(){
        Pair<Pair<Object, ColumnType>, Boolean> bound1 = leftCond.getBoundValue();
        Pair<Pair<Object, ColumnType>, Boolean> bound2 = rightCond.getBoundValue();
        String symbol;
        if (leftCond.expr1.getType() == 1) {
            symbol = leftCond.expr1.getSymbol();
        } else
        {
            symbol = leftCond.expr2.getSymbol();
        }
        if (leftCond.isLowerBounded(symbol)) {
            return new Pair<>(bound1, bound2);
        }
        return new Pair<>(bound2, bound1);
    }

    /*
        if this condition is in the form "symbol = value"
        params:
            symbol:   symbol's name
        return:
            true if is "symbol = value", otherwise false
     */
    public boolean isSymbolEqualSomething(String symbol) {
        if (type != 2) return false;
        if (op.compareTo("EQ") != 0) return false;
        return ((expr1.isSymbol(symbol) && expr2.isValue()) ||
                (expr2.isSymbol(symbol) && expr1.isValue()));
    }

    /*
        if this condition is in the form "symbol ∈ (-∞, x)” or "symbol ∈ (-∞, x]"
        params:
            symbol:   symbol's name
        return:
            true if is "symbol ∈ (-∞, x)” or "symbol ∈ (-∞, x]", otherwise false
     */
    public boolean isUpperBounded(String symbol) {
        if (type != 2) return false;
        if (op.compareTo("LT") == 0 || op.compareTo("NGT") == 0) {
            return expr1.isSymbol(symbol) && expr2.isNumericValue();
        }
        if (op.compareTo("GT") == 0 || op.compareTo("NLT") == 0) {
            return expr2.isSymbol(symbol) && expr1.isNumericValue();
        }
        return false;
    }

    /*
        if this condition is in the form "symbol ∈ (x, +∞)” or "symbol ∈ [x, +∞)"
        params:
            symbol:   symbol's name
        return:
            true if is "symbol ∈ (x, +∞)” or "symbol ∈ [x, +∞)", otherwise false
     */
    public boolean isLowerBounded(String symbol) {
        if (type != 2) return false;
        if (op.compareTo("GT") == 0 || op.compareTo("NLT") == 0) {
            return expr1.isSymbol(symbol) && expr2.isNumericValue();
        }
        if (op.compareTo("LT") == 0 || op.compareTo("NGT") == 0) {
            return expr2.isSymbol(symbol) && expr1.isNumericValue();
        }
        return false;
    }

    /*
        if this condition is in the form "symbol ∈ (/[x, y)/]"
        params:
            symbol:   symbol's name
        return:
            true if is "symbol ∈ (/[x, y)/]", otherwise false
     */
    public boolean isRanged(String symbol) {
        if (type != 0) return false;
        return (leftCond.isLowerBounded(symbol) && rightCond.isUpperBounded(symbol)) ||
                (rightCond.isLowerBounded(symbol) && leftCond.isUpperBounded(symbol));
    }

    /*
        if this condition is contains only single table/temptable
        you should pass table names list because of temptable
     */
    private boolean onlySingleTable2(ArrayList<String> tbNames) {
        switch (this.type) {
            case 0:
            case 1:
                return this.leftCond.onlySingleTable2(tbNames) &&
                        this.rightCond.onlySingleTable2(tbNames);
            case 2:
                return this.expr1.onlySingleTable(tbNames) &&
                        this.expr2.onlySingleTable(tbNames);
            default:
                throw new GrammarException("Unexpected Conditions type!");
        }
    }

    public boolean onlySingleTable(ArrayList<Table> tbs){
        ArrayList<String> tbNames = new ArrayList<>();
        tbNames.ensureCapacity(tbs.size());
        for (Table tb: tbs) {
            tbNames.add(tb.getTableName());
        }
        return onlySingleTable2(tbNames);
    }

    private boolean twoTablesEqual2(ArrayList<String> tbNames1, ArrayList<String> tbNames2) {
        switch (this.type) {
            case 0:
                return this.leftCond.twoTablesEqual2(tbNames1, tbNames2) &&
                        this.rightCond.twoTablesEqual2(tbNames1, tbNames2);
            case 1:
                return false;
            case 2:
                if (this.op.compareTo("EQ") == 0 &&
                        this.expr1.isSymbol() &&
                        this.expr2.isSymbol()) {
                    String t1 = this.expr1.getSymbol();
                    String t2 = this.expr2.getSymbol();
                    if (t1.indexOf(".") >= 0 && t2.indexOf(".") >= 0) {
                        t1 = t1.substring(0, t1.indexOf("."));
                        t2 = t2.substring(0, t2.indexOf("."));
                    }
                    return (tbNames1.indexOf(t1)>=0 && tbNames2.indexOf(t2)>=0) ||
                            (tbNames1.indexOf(t2)>=0 && tbNames2.indexOf(t1)>=0);
                }
                return false;
            default:
                throw new GrammarException("Unexpected Conditions type!");
        }
    }

    public boolean twoTablesEqual(ArrayList<Table> tbs1, ArrayList<Table> tbs2)  {
        ArrayList<String> tbNames1 = new ArrayList<>();
        ArrayList<String> tbNames2 = new ArrayList<>();
        tbNames1.ensureCapacity(tbs1.size());
        tbNames2.ensureCapacity(tbs2.size());
        for (Table tb: tbs1) {
            tbNames1.add(tb.getTableName());
        }
        for (Table tb: tbs2) {
            tbNames2.add(tb.getTableName());
        }
        return twoTablesEqual2(tbNames1, tbNames2);
    }

    public Pair<ArrayList<String>, ArrayList<String>> getTwoTableColumns2(ArrayList<String> tbNames1, ArrayList<String> tbNames2) {
        switch (this.type) {
            case 0:
                Pair<ArrayList<String>, ArrayList<String>> res1 = this.leftCond.getTwoTableColumns2(tbNames1, tbNames2);
                Pair<ArrayList<String>, ArrayList<String>> res2 = this.rightCond.getTwoTableColumns2(tbNames1, tbNames2);
                ArrayList<String> l1 = res1.getKey(), l2 = res1.getValue();
                l1.addAll(res2.getKey());
                l2.addAll(res2.getValue());
                return new Pair<>(l1, l2);
            case 1:
                throw new GrammarException("Please call twoTablesEqual() first!");
            case 2:
                if (this.op.compareTo("EQ") == 0 &&
                        this.expr1.isSymbol() &&
                        this.expr2.isSymbol()) {
                    String t1 = this.expr1.getSymbol();
                    String t2 = this.expr2.getSymbol();
                    if (t1.indexOf(".") >= 0 && t2.indexOf(".") >= 0) {
                        t1 = t1.substring(0, t1.indexOf("."));
                        t2 = t2.substring(0, t2.indexOf("."));
                    }
                    ArrayList<String> p1 = new ArrayList<>();
                    ArrayList<String> p2 = new ArrayList<>();
                    if (tbNames1.indexOf(t1)>=0 && tbNames2.indexOf(t2)>=0) {
                        p1.add(this.expr1.getSymbol());
                        p2.add(this.expr2.getSymbol());
                        return new Pair<>(p1, p2);
                    } else
                    if (tbNames1.indexOf(t2)>=0 && tbNames2.indexOf(t1)>=0) {
                        p1.add(this.expr2.getSymbol());
                        p2.add(this.expr1.getSymbol());
                        return new Pair<>(p1, p2);
                    }
                    throw new GrammarException("Please call twoTablesEqual() first!");
                }
            default:
                throw new GrammarException("Unexpected Conditions type!");
        }
    }

    public Pair<ArrayList<String>, ArrayList<String>> getTwoTableColumns(ArrayList<Table> tbs1, ArrayList<Table> tbs2)  {
        ArrayList<String> tbNames1 = new ArrayList<>();
        ArrayList<String> tbNames2 = new ArrayList<>();
        tbNames1.ensureCapacity(tbs1.size());
        tbNames2.ensureCapacity(tbs2.size());
        for (Table tb: tbs1) {
            tbNames1.add(tb.getTableName());
        }
        for (Table tb: tbs2) {
            tbNames2.add(tb.getTableName());
        }
        return getTwoTableColumns2(tbNames1, tbNames2);
    }


    private boolean check(Object obj1, Object obj2, String expectedRelation, ColumnType type) {
        int comp = ColumnType.compare(obj1, obj2, type);
        switch (expectedRelation) {
            case "EQ":
                return comp == 0;
            case "NEQ":
                return comp == -1 || comp == 1;
            case "LT":
                return comp == -1;
            case "GT":
                return comp == 1;
            case "NLT":
                return comp == 0 || comp == 1;
            case "NGT":
                return comp == 0 || comp == -1;
            default:
                throw new GrammarException("Unknown relation!");
        }
    }

}
