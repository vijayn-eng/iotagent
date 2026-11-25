package io.confluent.proddemo;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Raise an expedite request for a short material (ERP / warehouse replenishment).
 *
 * <p>Mock tool: returns a confirmation with no real side effect.
 */
public class ExpediteMaterial extends ScalarFunction {
    public String eval(String materialId, String quantity, String reason) {
        return "ERP-EXP-" + Ids.shortId(materialId + quantity) + ": expedite raised for "
                + quantity + " units of " + materialId + " (priority freight, hot-list). Reason: " + reason;
    }
}
