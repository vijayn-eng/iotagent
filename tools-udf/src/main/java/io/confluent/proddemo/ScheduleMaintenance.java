package io.confluent.proddemo;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Open a maintenance work order in the CMMS for a machine.
 *
 * <p>Mock tool: returns a confirmation with no real side effect.
 */
public class ScheduleMaintenance extends ScalarFunction {
    public String eval(String machineId, String window, String reason) {
        return "CMMS-WO-" + Ids.shortId(machineId + window) + ": maintenance scheduled on "
                + machineId + " (" + window + "). Technician dispatched. Reason: " + reason;
    }
}
