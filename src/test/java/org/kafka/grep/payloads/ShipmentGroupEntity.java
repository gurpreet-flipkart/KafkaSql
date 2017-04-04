package org.kafka.grep.payloads;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
@NoArgsConstructor
public class ShipmentGroupEntity {
    String vendor_tracking_id;
    String status;
    AddressEntity destination_location;
    AddressEntity source_location;
    AddressEntity current_location;
    List<String> shipments;
    String type;
}
