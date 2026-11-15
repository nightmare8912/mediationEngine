package org.prats.definitions.records;


import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the successful result of a database
 * operation.
 *
 * SELECT:
 *     rows contains the retrieved records.
 *
 * INSERT / UPDATE / DELETE:
 *     affectedRows indicates the number of
 *     successfully affected database rows.
 *
 * Errors are represented using the standard
 * Message error structure.
 */
public record DatabaseResponse(
        List<Map<String, Object>> rows,
        long affectedRows
) {

    public DatabaseResponse {

        if (affectedRows < 0) {
            throw new IllegalArgumentException(
                    "Affected row count cannot be negative"
            );
        }

        /*
         * Preserve SQL NULL values while preventing
         * callers from modifying the returned rows.
         */
        if (rows == null) {

            rows = List.of();

        } else {

            List<Map<String, Object>> copy =
                    new ArrayList<>(rows.size());

            for (Map<String, Object> row : rows) {

                copy.add(
                        Collections.unmodifiableMap(
                                new LinkedHashMap<>(row)
                        )
                );
            }

            rows = List.copyOf(copy);
        }
    }
}
