package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.CustomizedException;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;


import java.util.ArrayList;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Expression {
    private int TypeNum; // 0  numeric value
    // 1  symbol
    // 2  unary_op expr
    // 3  expr op expr
    // 4  string value
    private String symbolORValue;
    private ColumnType valueColumnType;
    private Expression expr1, expr2;
    private String op; // +, -, *, /, %, ~, not

    public Expression(int TypeNum, String symbolORvalue) throws CustomizedException {
        if (TypeNum != 0 && TypeNum != 1 && TypeNum != 4)
            throw new CustomizedException("Exception: it is not a basic type");

        this.TypeNum = TypeNum;
        if (TypeNum == 0) {
            if (!isNumeric(symbolORvalue)) {
                throw new CustomizedException("Exception: " + symbolORvalue + "is not a number");
            }
            this.symbolORValue = symbolORvalue;
            if (isInteger(symbolORValue)) {
                if (symbolORvalue.compareTo(String.valueOf(Integer.parseInt(symbolORvalue))) == 0)
                    this.valueColumnType = ColumnType.INT;
                else
                    this.valueColumnType = ColumnType.LONG;
            } else {
                if (symbolORvalue.compareTo(String.valueOf(Float.parseFloat(symbolORvalue))) == 0)
                    this.valueColumnType = ColumnType.FLOAT;
                else
                    this.valueColumnType = ColumnType.DOUBLE;
            }
        } else
        if (TypeNum == 1) {
            this.symbolORValue = symbolORvalue;
            this.valueColumnType = null;
        } else
        {
            this.symbolORValue = symbolORvalue;
            this.valueColumnType = ColumnType.STRING;
        }
    }

    public Expression(int TypeNum, String unary_op, Expression expr) throws CustomizedException  {
        if (TypeNum != 2) {
            throw new CustomizedException("Exception: it is not a basic type");
        }

        this.TypeNum = TypeNum;
        this.expr1 = expr;
        this.op = unary_op;
        this.valueColumnType = null;
//        }
    }

    public Expression(int TypeNum, String op, Expression expr1, Expression expr2)
            throws CustomizedException {
        if (TypeNum != 3) {
            throw new CustomizedException("Exception: it is not type for expression");
        }
        this.TypeNum = TypeNum;
        this.op = op;
        this.expr1 = expr1;
        this.expr2 = expr2;
    }

    /*
        calculate this expression's value
        params:
            nameList:   symbols' name
            TypeNumList:   symbols' ColumnTypes
            valueList:  symbols' values
        return:
            one Pair<> value p, p's first value is real numeric/string, p's second value is its ColumnType
     */
    public Pair<Object, ColumnType> calcValue(LinkedList<String> nameList, LinkedList<ColumnType> ColumnTypeList, LinkedList valueList) {
        switch (TypeNum) {
            case 0:
                Object obj = ColumnType.convert(this.symbolORValue, this.valueColumnType);
                return new Pair<>(obj, this.valueColumnType);
            case 1:
                String tablename, attrname;
                int idx;
                //如果需要的话，把“表名.属性”分开
                if(!nameList.get(0).contains(".") && this.symbolORValue.contains(".")) {
                    attrname = symbolORValue.split("\\.")[1];
                    idx = nameList.indexOf((attrname));
                }
                else {
                    idx = nameList.indexOf(this.symbolORValue);
                }
                if (idx == -1)
                    throw new CustomizedException("Unknown column name '" + this.symbolORValue + "'!");
                return new Pair<>(valueList.get(idx), ColumnTypeList.get(idx));
            case 2:
                Pair<Object, ColumnType> tmp = expr1.calcValue(nameList, ColumnTypeList, valueList);
                switch (this.op) {
                    case "+":
                        return tmp;
                    case "-":
                        return neg(tmp);
                    case "~":
                        return non(tmp);
                    case "NOT":
                        return not(tmp);
                    default:
                        throw new CustomizedException("Unexpected unary operation '" + this.op + "' !");
                }
            case 3:
                Pair<Object, ColumnType> tmp1 = expr1.calcValue(nameList, ColumnTypeList, valueList);
                Pair<Object, ColumnType> tmp2 = expr2.calcValue(nameList, ColumnTypeList, valueList);
                ColumnType finalColumnType = ColumnType.lift(tmp1.getValue(), tmp2.getValue());
                Object o1 = ColumnType.convert(tmp1.getKey(), finalColumnType);
                Object o2 = ColumnType.convert(tmp2.getKey(), finalColumnType);
                switch (this.op) {
                    case "+":
                        return plus(o1, o2, finalColumnType);
                    case "-":
                        return minus(o1, o2, finalColumnType);
                    case "*":
                        return multiply(o1, o2, finalColumnType);
                    case "/":
                        return divide(o1, o2, finalColumnType);
                    case "%":
                        return mod(o1, o2, finalColumnType);
                    default:
                        throw new CustomizedException("Unexpected binary operation '" + this.op + "' !");
                }
            case 4:
                return new Pair<>(this.symbolORValue, this.valueColumnType);
            default:
                throw new CustomizedException("Unknown Expression ColumnType!");
        }
    }

    /*
        test if it is a symbol that is equal to "symbol"
        params:
            nameList:   symbols' name
        return:
            true if satisfied, otherwise false
     */
    public boolean isSymbol(String symbol) {
        return (TypeNum == 1 && symbolORValue.compareTo(symbol) == 0);
    }

    public boolean isSymbol() {
        return TypeNum == 1;
    }

    /*
        test if it is a numeric value
        params:
            none
        return:
            true if satisfied, otherwise false
     */
    public boolean isNumericValue() {
        return TypeNum == 0;
    }

    /*
        test if it is a string value
        params:
            none
        return:
            true if satisfied, otherwise false
     */
    public boolean isStringValue() {
        return TypeNum == 4;
    }

    /*
        test if it is a string/numeric value
        params:
            none
        return:
            true if satisfied, otherwise false
     */
    public boolean isValue() {
        return isNumericValue() || isStringValue();
    }

    //判断字符串是否是数字
    public boolean isNumeric(String value){
        Pattern pattern = Pattern.compile("-?[0-9]+\\.?[0-9]*");
        Matcher matcher = pattern.matcher(value);
        return matcher.matches();
    }

    //判断字符串是否是整数
    public boolean isInteger(String value){
        Pattern pattern = Pattern.compile("-?[0-9]+");
        Matcher matcher = pattern.matcher(value);
        return matcher.matches();
    }

    /*
        get string/numeric value directly
        params:
            none
        return:
            one Pair<> value p, p's first value is real numeric/string, p's second value is its ColumnType
        * Note: make sure isValue() returns true before you call this function
     */
    public Pair<Object, ColumnType> getDirectValue() {
        if (TypeNum == 0) {
            Object obj = ColumnType.convert(this.symbolORValue, this.valueColumnType);
            return new Pair<>(obj, this.valueColumnType);
        } else
        if (TypeNum == 4) {
            return new Pair<>(this.symbolORValue, this.valueColumnType);
        }
        throw new CustomizedException("Can't get value directly!");
    }

    /*
        simplify this Expression
        params: none
        return: a simplified expression
     */
    public Expression simplify() {
        if (TypeNum == 0 || TypeNum == 1 || TypeNum == 4)
            return this;
        if (TypeNum == 2) {
            Expression expr = expr1.simplify();
            if (expr.isNumericValue()) {
                switch (this.op) {
                    case "+":
                        return expr;
                    case "-":
                        return new Expression(0, neg(expr.getDirectValue()).getKey().toString());
                    case "~":
                        return new Expression(0, non(expr.getDirectValue()).getKey().toString());
                    case "NOT":
                        return new Expression(0, not(expr.getDirectValue()).getKey().toString());
                    default:
                        throw new CustomizedException("Unexpected unary operation '" + this.op + "' !");
                }
            }
            return new Expression(2, this.op, expr);
        }
        Expression expr1 = this.expr1.simplify();
        Expression expr2 = this.expr2.simplify();
        if (expr1.isNumericValue() && expr2.isNumericValue()) {
            Pair<Object, ColumnType> x = expr1.getDirectValue();
            Pair<Object, ColumnType> y = expr1.getDirectValue();
            ColumnType finalColumnType = ColumnType.lift(x.getValue(), y.getValue());
            Object o1 = ColumnType.convert(x.getKey(), finalColumnType);
            Object o2 = ColumnType.convert(y.getKey(), finalColumnType);
            switch (this.op) {
                case "+":
                    return new Expression(0, plus(o1, o2, finalColumnType).getKey().toString());
                case "-":
                    return new Expression(0, minus(o1, o2, finalColumnType).getKey().toString());
                case "*":
                    return new Expression(0, multiply(o1, o2, finalColumnType).getKey().toString());
                case "/":
                    return new Expression(0, divide(o1, o2, finalColumnType).getKey().toString());
                case "%":
                    return new Expression(0, mod(o1, o2, finalColumnType).getKey().toString());
                default:
                    throw new CustomizedException("Unexpected binary operation '" + this.op + "' !");
            }
        }
        if (this.op.compareTo("+") == 0 && expr1.isStringValue() && expr2.isStringValue()) {
            Pair<Object, ColumnType> x = expr1.getDirectValue();
            Pair<Object, ColumnType> y = expr1.getDirectValue();
            return new Expression(4, x.getKey().toString()+y.getKey().toString());
        }
        return new Expression(3, this.op, expr1, expr2);
    }

    public boolean onlySingleTable(ArrayList<String> tbNames){
        switch (TypeNum) {
            case 0:
            case 4:
                return true;
            case 1:
                int idx = this.symbolORValue.indexOf(".");
                return idx >= 0 && tbNames.indexOf(this.symbolORValue.substring(0, idx)) >= 0;
            case 2:
                return this.expr1.onlySingleTable(tbNames);
            case 3:
                return this.expr1.onlySingleTable(tbNames) &&
                        this.expr2.onlySingleTable(tbNames);
            default:
                throw new CustomizedException("Unknown Expression ColumnType!");
        }
    }

    public String getSymbol() {
        if (TypeNum != 1) {
            throw new CustomizedException("Not a symbol expression!");
        }
        return symbolORValue;
    }

    /*
        convert column name to table_name.column_name
        params: table list
        return: none
     */
    public void normalize(ArrayList<Table> tables, LinkedList<String> ignoreNames) {
        switch (TypeNum) {
            case 1:
                if (this.symbolORValue.indexOf(".") == -1) {
                    String tableName = null;
                    for (Table table : tables) {
                        if (table.getColNames().indexOf(this.symbolORValue) != -1 &&
                                (ignoreNames == null || !ignoreNames.contains(table.getTableName() + "." + this.symbolORValue))) {
                            if (tableName != null)
                                throw new CustomizedException("Column name '" + this.symbolORValue +"' occurs in Table '" +
                                        tableName + "' and Table '" + table.getTableName() + "'!");
                            tableName = table.getTableName();
                        }
                    }
                    if (tableName == null)
                        throw new CustomizedException("Unknown column name: '" + this.symbolORValue + "'");
                    this.symbolORValue = tableName + "." + this.symbolORValue;
                }
                break;
            case 2:
                this.expr1.normalize(tables);
                break;
            case 3:
                this.expr1.normalize(tables);
                this.expr2.normalize(tables);
                break;
        }
    }

    public void normalize(ArrayList<Table> tables) {
        normalize(tables, null);
    }

    public int getType() {return this.TypeNum;}
    public ColumnType getValueColumnType() {return this.valueColumnType;}

    //负号处理
    public static Pair<Object, ColumnType> neg(Pair<Object, ColumnType> p) {
        switch (p.getValue()) {
            case INT:
                return new Pair<>(-(int)p.getKey(), p.getValue());
            case LONG:
                return new Pair<>(-(long)p.getKey(), p.getValue());
            case FLOAT:
                return new Pair<>(-(float)p.getKey(), p.getValue());
            case DOUBLE:
                return new Pair<>(-(double)p.getKey(), p.getValue());
            default:
                throw new CustomizedException("Unsupported NEG operation!");
        }
    }

    public static Pair<Object, ColumnType> non(Pair<Object, ColumnType> p) {
        switch (p.getValue()) {
            case INT:
                return new Pair<>(~(int)p.getKey(), p.getValue());
            case LONG:
                return new Pair<>(~(long)p.getKey(), p.getValue());
            default:
                throw new CustomizedException("Unsupported NON operation!");
        }
    }

    public static Pair<Object, ColumnType> not(Pair<Object, ColumnType> p) {
        switch (p.getValue()) {
            case INT:
                return new Pair<>((int)p.getKey() == 0 ? 1 : 0, ColumnType.INT);
            case LONG:
                return new Pair<>((long)p.getKey() == 0 ? 1 : 0, ColumnType.INT);
            case FLOAT:
                return new Pair<>((float)p.getKey() == 0 ? 1 : 0, ColumnType.INT);
            case DOUBLE:
                return new Pair<>((double)p.getKey() == 0 ? 1 : 0, ColumnType.INT);
            default:
                throw new CustomizedException("Unsupported NOT operation!");
        }
    }

    //加法运算
    public static Pair<Object, ColumnType> plus(Object o1, Object o2, ColumnType finalColumnType) {
        switch (finalColumnType) {
            case INT:
                return new Pair<>((int)o1+(int)o2, finalColumnType);
            case LONG:
                return new Pair<>((long)o1+(long)o2, finalColumnType);
            case FLOAT:
                return new Pair<>((float)o1+(float)o2, finalColumnType);
            case DOUBLE:
                return new Pair<>((double)o1+(double)o2, finalColumnType);
            default:
                throw new CustomizedException("Unsupported PLUS operation!");
        }
    }

    //减法运算
    public static Pair<Object, ColumnType> minus(Object o1, Object o2, ColumnType finalColumnType) {
        switch (finalColumnType) {
            case INT:
                return new Pair<>((int)o1-(int)o2, finalColumnType);
            case LONG:
                return new Pair<>((long)o1-(long)o2, finalColumnType);
            case FLOAT:
                return new Pair<>((float)o1-(float)o2, finalColumnType);
            case DOUBLE:
                return new Pair<>((double)o1-(double)o2, finalColumnType);
            default:
                throw new CustomizedException("Unsupported MINUS operation!");
        }
    }

    //乘法运算
    public static Pair<Object, ColumnType> multiply(Object o1, Object o2, ColumnType finalColumnType) {
        switch (finalColumnType) {
            case INT:
                return new Pair<>((int)o1*(int)o2, finalColumnType);
            case LONG:
                return new Pair<>((long)o1*(long)o2, finalColumnType);
            case FLOAT:
                return new Pair<>((float)o1*(float)o2, finalColumnType);
            case DOUBLE:
                return new Pair<>((double)o1*(double)o2, finalColumnType);
            default:
                throw new CustomizedException("Unsupported MULTIPLY operation!");
        }
    }

    //除法运算
    public static Pair<Object, ColumnType> divide(Object o1, Object o2, ColumnType finalColumnType) {
        switch (finalColumnType) {
            case INT:
                return new Pair<>((int)o1/(int)o2, finalColumnType);
            case LONG:
                return new Pair<>((long)o1/(long)o2, finalColumnType);
            case FLOAT:
                return new Pair<>((float)o1/(float)o2, finalColumnType);
            case DOUBLE:
                return new Pair<>((double)o1/(double)o2, finalColumnType);
            default:
                throw new CustomizedException("Unsupported DIVIDE operation!");
        }
    }

    public static Pair<Object, ColumnType> mod(Object o1, Object o2, ColumnType finalColumnType) {
        switch (finalColumnType) {
            case INT:
                return new Pair<>((int)o1%(int)o2, finalColumnType);
            case LONG:
                return new Pair<>((long)o1%(long)o2, finalColumnType);
            case FLOAT:
                return new Pair<>((float)o1%(float)o2, finalColumnType);
            case DOUBLE:
                return new Pair<>((double)o1%(double)o2, finalColumnType);
            default:
                throw new CustomizedException("Unsupported MOD operation!");
        }
    }
}
