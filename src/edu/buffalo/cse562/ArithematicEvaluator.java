/**
 * 
 */

package edu.buffalo.cse562;

import java.math.BigDecimal;

/**
 * @author Pratik
 */
public class ArithematicEvaluator {
    public double calculate(String s) {
        try {
            s = s.trim();
            double result = 0.0;

            String equations[] = s.split("\\(");
            if (equations.length == 1) {
                equations[0] = equations[0].trim();
                result = solve(equations[0]);
            } else {
                char curOp = '|';
                char nxtOp = '|';
                int count = 0;
                for (String equation : equations) {
                    equation = equation.trim();
                    curOp = nxtOp;
                    equation = equation.replaceAll("\\)", "");
                    if (count != equations.length - 1) {
                        nxtOp = equation.charAt(equation.length() - 1);
                        equation = equation.substring(0, equation.length() - 1);
                    }
                    equation = equation.trim();

                    double tempresult = solve(equation);

                    if (count != 0) {
                        if (curOp == '*') {
                            result = result * tempresult;
                        } else if (curOp == '+') {
                            result = result + tempresult;
                        } else if (curOp == '-') {
                            result = result - tempresult;
                        } else if (curOp == '/') {
                            result = result / tempresult;
                        }
                    } else {
                        result = tempresult;
                    }
                    count++;
                }

            }
            return result;
        } catch (Exception e) {
            return Double.POSITIVE_INFINITY;
        }
    }

    public double solve(String equation) {

        double result;

        if (equation.contains("*")) {

            double lhs = Double.parseDouble(equation.split("\\*")[0]);
            double rhs = Double.parseDouble(equation.split("\\*")[1]);
            result = lhs * rhs;
            return result;

        } else if (equation.contains("+")) {

            double lhs = Double.parseDouble(equation.split("\\+")[0]);
            double rhs = Double.parseDouble(equation.split("\\+")[1]);
            BigDecimal d1 = new BigDecimal(String.valueOf(lhs));
            BigDecimal d2 = new BigDecimal(String.valueOf(rhs));
            result = d1.add(d2).doubleValue();
            return result;

        } else if (equation.contains("-")) {

            double lhs = Double.parseDouble(equation.split("\\-")[0]);
            double rhs = Double.parseDouble(equation.split("\\-")[1]);
            BigDecimal d1 = new BigDecimal(String.valueOf(lhs));
            BigDecimal d2 = new BigDecimal(String.valueOf(rhs));
            result = d1.subtract(d2).doubleValue();
            return result;

        } else if (equation.contains("/")) {

            double lhs = Double.parseDouble(equation.split("\\/")[0]);
            double rhs = Double.parseDouble(equation.split("\\/")[1]);
            result = lhs / rhs;
            return result;
        } else {

            result = Double.parseDouble(equation);
            return result;

        }

    }
}
