package org.utd.faradaybase.operation;

import lombok.Data;
import org.utd.faradaybase.DataType;

@Data
public class Operation {
    private String columnName;
    private OperandType operator;
    private String comparatorValue;
    private boolean negation;
    private int columnOrdinal;
    private DataType dataType;

    public Operation(DataType dataType) {
        this.dataType = dataType;
    }

    public static String[] supportedOperators = {"<=", ">=", "<>", ">", "<", "="};

    public static OperandType get_typeOf_operator(String strOperator) {
        switch (strOperator) {
            case ">":
                return OperandType.GREATERTHAN;
            case "<":
                return OperandType.LESSTHAN;
            case "=":
                return OperandType.EQUALTO;
            case ">=":
                return OperandType.GREATERTHANOREQUAL;
            case "<=":
                return OperandType.LESSTHANOREQUAL;
            case "<>":
                return OperandType.NOTEQUAL;
            default:
                System.out.println("! Invalid operator \"" + strOperator + "\"");
                return OperandType.INVALID;
        }
    }

    public static int compare(String value1, String value2, DataType data_type) {
        if (data_type == DataType.TEXT)
            return value1.toLowerCase().compareTo(value2);
        else if (data_type == DataType.NULL) {
            if (value1 == value2)
                return 0;
            else if (value1.equalsIgnoreCase("null"))
                return 1;
            else
                return -1;
        } else {
            return Long.valueOf(Long.parseLong(value1) - Long.parseLong(value2)).intValue();
        }
    }

    public boolean condition_check(String currentValue) {
        OperandType operation = getOperation();
        if (currentValue.equalsIgnoreCase("null")
                || comparatorValue.equalsIgnoreCase("null"))
            return on_difference_operation(operation, compare(currentValue, comparatorValue, DataType.NULL));
        if (dataType == DataType.TEXT || dataType == DataType.NULL)
            return compare_string(currentValue, operation);
        else {
            switch (operation) {
                case LESSTHANOREQUAL:
                    return Long.parseLong(currentValue) <= Long.parseLong(comparatorValue);
                case GREATERTHANOREQUAL:
                    return Long.parseLong(currentValue) >= Long.parseLong(comparatorValue);

                case NOTEQUAL:
                    return Long.parseLong(currentValue) != Long.parseLong(comparatorValue);
                case LESSTHAN:
                    return Long.parseLong(currentValue) < Long.parseLong(comparatorValue);

                case GREATERTHAN:
                    return Long.parseLong(currentValue) > Long.parseLong(comparatorValue);
                case EQUALTO:
                    return Long.parseLong(currentValue) == Long.parseLong(comparatorValue);

                default:
                    return false;
            }
        }
    }

    public void setConditionValue(String conditionValue) {
        this.comparatorValue = conditionValue;
        this.comparatorValue = comparatorValue.replace("'", "");
        this.comparatorValue = comparatorValue.replace("\"", "");

    }

    public void setColumName(String columnName) {
        this.columnName = columnName;
    }

    public void setOperator(String operator) {
        this.operator = get_typeOf_operator(operator);
    }

    public void setNegation(boolean negate) {
        this.negation = negate;
    }

    public OperandType getOperation() {
        if (!negation)
            return this.operator;
        else
            return negateOperator();
    }

    private boolean on_difference_operation(OperandType operation, int difference) {
        switch (operation) {
            case LESSTHANOREQUAL:
                return difference <= 0;
            case GREATERTHANOREQUAL:
                return difference >= 0;
            case NOTEQUAL:
                return difference != 0;
            case LESSTHAN:
                return difference < 0;
            case GREATERTHAN:
                return difference > 0;
            case EQUALTO:
                return difference == 0;
            default:
                return false;
        }
    }

    private boolean compare_string(String currentValue, OperandType operation) {
        return on_difference_operation(operation, currentValue.toLowerCase().compareTo(comparatorValue));
    }

    private OperandType negateOperator() {
        switch (this.operator) {
            case LESSTHANOREQUAL:
                return OperandType.GREATERTHAN;
            case GREATERTHANOREQUAL:
                return OperandType.LESSTHAN;
            case NOTEQUAL:
                return OperandType.EQUALTO;
            case LESSTHAN:
                return OperandType.GREATERTHANOREQUAL;
            case GREATERTHAN:
                return OperandType.LESSTHANOREQUAL;
            case EQUALTO:
                return OperandType.NOTEQUAL;
            default:
                System.out.println("! Invalid operator \"" + this.operator + "\"");
                return OperandType.INVALID;
        }
    }
}
