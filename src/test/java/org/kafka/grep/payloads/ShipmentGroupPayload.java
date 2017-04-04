package org.kafka.grep.payloads;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonRootName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@JsonRootName("entity")
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShipmentGroupPayload {
    long ingestedAt;
    String entityId;
    long updatedAt;
    ShipmentGroupEntity data;
}
