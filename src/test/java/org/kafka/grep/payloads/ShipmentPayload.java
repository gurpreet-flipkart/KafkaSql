package org.kafka.grep.payloads;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonRootName;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonRootName("entity")
public class ShipmentPayload{

    long ingestedAt;
    String entityId;
    long updatedAt;
    ShipmentEntity data;
}
