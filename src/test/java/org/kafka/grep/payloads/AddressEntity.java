package org.kafka.grep.payloads;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
public class AddressEntity {
    String id;
    String type;
    String pincode;

    public AddressEntity(String id) {
        this.id = id;
    }

}
