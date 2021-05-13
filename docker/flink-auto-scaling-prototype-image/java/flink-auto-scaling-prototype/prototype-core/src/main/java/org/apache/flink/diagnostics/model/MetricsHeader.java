package org.apache.flink.diagnostics.model;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;


/**
 * TODO:
 *  1. Do we need operator level information
 *  2. Check whether we need to change this for k8s
 */
@ToString
@EqualsAndHashCode
public class MetricsHeader extends HashMap<String, String> {

}
