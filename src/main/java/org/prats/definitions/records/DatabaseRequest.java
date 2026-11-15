package org.prats.definitions.records;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a structured database request.
 *
 * The enclosing Message determines the database
 * operation: FETCH, SEARCH, CREATE, UPDATE or DELETE.
 *
 * This record stores only the database-specific
 * request parameters.
 *
 * Jackson serializes the record into a JsonNode
 * for transmission through MessageService.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DatabaseRequest(
        String entity,
        List<String> columns,
        Map<String, Object> data,
        Filter filters
) {

    // =========================================================
    // 1. REQUEST VALIDATION
    // =========================================================

    public DatabaseRequest {

        Objects.requireNonNull(
                entity,
                "Database entity cannot be null"
        );

        if (entity.isBlank()) {
            throw new IllegalArgumentException(
                    "Database entity cannot be blank"
            );
        }

        /*
         * Defensive copies prevent callers from
         * modifying the request's collections
         * after construction.
         *
         * The SQL parser performs operation-specific
         * validation before generating SQL.
         */
        columns = columns == null
                ? List.of()
                : List.copyOf(columns);

        data = data == null
                ? Map.of()
                : Map.copyOf(data);
    }

    // =========================================================
    // 2. COMPARISON OPERATORS
    // =========================================================

    public enum ComparisonOperator {
        EQ,
        NE,
        GT,
        GE,
        LT,
        LE,
        LIKE
    }

    // =========================================================
    // 3. LOGICAL OPERATORS
    // =========================================================

    public enum LogicalOperator {
        AND,
        OR
    }

    // =========================================================
    // 4. FILTER INTERFACE
    // =========================================================

    /**
     * A filter can be:
     *
     * 1. A comparison condition.
     * 2. A logical group containing filters.
     *
     * Explicit subtype names allow Jackson to
     * reconstruct the correct record from JSON.
     */
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "type"
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(
                    value = Condition.class,
                    name = "condition"
            ),
            @JsonSubTypes.Type(
                    value = Group.class,
                    name = "group"
            )
    })
    public sealed interface Filter
            permits Condition, Group {
    }

    // =========================================================
    // 5. COMPARISON CONDITION
    // =========================================================

    /**
     * Represents an individual comparison.
     *
     * Example:
     *
     * AGE > 20
     *
     * field    = "AGE"
     * operator = GT
     * value    = 20
     *
     * The SQL parser must accept only supported
     * scalar value types and bind them as parameters.
     */
    public record Condition(
            String field,
            ComparisonOperator operator,
            Object value
    ) implements Filter {

        public Condition {

            Objects.requireNonNull(
                    field,
                    "Condition field cannot be null"
            );

            Objects.requireNonNull(
                    operator,
                    "Comparison operator cannot be null"
            );

            Objects.requireNonNull(
                    value,
                    "Comparison value cannot be null"
            );

            if (field.isBlank()) {
                throw new IllegalArgumentException(
                        "Condition field cannot be blank"
                );
            }
        }
    }

    // =========================================================
    // 6. LOGICAL GROUP
    // =========================================================

    /**
     * Combines multiple Filter objects.
     *
     * A group can contain Condition objects
     * and other Group objects.
     *
     * This allows recursive filter expressions.
     */
    public record Group(
            LogicalOperator operator,
            List<Filter> children
    ) implements Filter {

        public Group {

            Objects.requireNonNull(
                    operator,
                    "Logical operator cannot be null"
            );

            Objects.requireNonNull(
                    children,
                    "Filter children cannot be null"
            );

            if (children.isEmpty()) {
                throw new IllegalArgumentException(
                        "Filter group cannot be empty"
                );
            }

            children = List.copyOf(children);
        }
    }
}

