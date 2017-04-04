package org.kafka.grep.payloads;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShipmentEntity {
    String updated_at;
    String status;
    String vendor_tracking_id;
    AddressEntity current_address;
    AddressEntity source_address;
    AddressEntity destination_address;
    String shipment_type;

    public ShipmentEntity(String updated_at,
                          String status,
                          String vendor_tracking_id,
                          AddressEntity current_address) {

        this.updated_at = updated_at;
        this.status = status;
        this.vendor_tracking_id = vendor_tracking_id;
        this.current_address = current_address;
    }


}
