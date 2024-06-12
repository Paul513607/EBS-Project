package org.ebs.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class Subscription implements Serializable {
    private Map<String, BiPredicate<Object, Object>> criteria;
    private List<String> fields;
    private List<String> operators;
    private List<Object> values;
    private Map<String, Object> fieldToValue;

    public Subscription() {
        criteria = new HashMap<>();
        fields = new ArrayList<>();
        operators = new ArrayList<>();
        values = new ArrayList<>();
        fieldToValue = new HashMap<>();
    }

    public void addCriterion(String field, String operator, Object value) {
        fields.add(field);
        operators.add(operator);
        values.add(value);
        fieldToValue.put(field, value);
        BiPredicate<Object, Object> predicate;
        switch (operator) {
            case "=":
                predicate = Object::equals;
                break;
            case ">":
                predicate = (a, b) -> ((Comparable) a).compareTo(b) < 0;
                break;
            case ">=":
                predicate = (a, b) -> ((Comparable) a).compareTo(b) <= 0;
                break;
            case "<":
                predicate = (a, b) -> ((Comparable) a).compareTo(b) > 0;
                break;
            case "<=":
                predicate = (a, b) -> ((Comparable) a).compareTo(b) >= 0;
                break;
            default:
                throw new IllegalArgumentException("Unknown operator: " + operator);
        }
        criteria.put(field, predicate);
    }

    public boolean matches(String company, double value, double drop, double variation, String date) {
        for (Map.Entry<String, BiPredicate<Object, Object>> entry : criteria.entrySet()) {
            String field = entry.getKey();
            BiPredicate<Object, Object> predicate = entry.getValue();
            Object fieldValue;
            Object subscriptionFieldValue;

            switch (field) {
                case "company":
                    fieldValue = company;
                    subscriptionFieldValue = fieldToValue.get("company");
                    break;
                case "value":
                    fieldValue = value;
                    subscriptionFieldValue = fieldToValue.get("value");
                    break;
                case "drop":
                    fieldValue = drop;
                    subscriptionFieldValue = fieldToValue.get("drop");
                    break;
                case "variation":
                    fieldValue = variation;
                    subscriptionFieldValue = fieldToValue.get("variation");
                    break;
                case "date":
                    fieldValue = date;
                    subscriptionFieldValue = fieldToValue.get("date");
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field: " + field);
            }

            if (!predicate.test(subscriptionFieldValue, fieldValue)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Subscription{");
        int i;
        for (i = 0; i < fields.size() - 1; i++) {
            sb.append("(" + fields.get(i) + "," + operators.get(i) + "," + values.get(i) + ");");
        }
        sb.append("(" + fields.get(i) + "," + operators.get(i) + "," + values.get(i) + ")}");

        return sb.toString();
    }
}

