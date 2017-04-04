package org.kafka.grep.grepper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

public class ParsingTest {
    @Test
    public void testPayload() throws IOException {
        String payload = "{\"entity\":{\"traceId\":\"f48ca9d2-c4e9-4802-b70e-452d625411b9\",\"schemaVersion\":\"3.2\",\"data\":{\"vendor_tracking_id\":\"RMAAPAD-1656857\",\"eta\":\"2017-02-04T02:33:19.000+05:30\",\"status\":\"CLOSED\",\"destination_location\":{\"id\":\"61\",\"type\":\"DELIVERY_HUB\"},\"shipment_groups\":[\"Bag-29216101\"],\"shipments\":[\"FMPC0177814930\",\"FMPC0177818906\",\"FMPC0177846578\",\"FMPC0177878888\",\"FMPC0177885845\",\"FMPC0177905709\",\"FMPC0177924324\",\"FMPC0177925059\",\"FMPC0177931182\",\"FMPC0177932659\",\"FMPC0177935549\",\"FMPC0177941638\",\"FMPC0177943658\",\"FMPC0177959906\",\"FMPC0177970378\",\"FMPC0177984793\",\"FMPC0177987150\",\"FMPC0177988138\",\"FMPC0177988364\",\"FMPC0177991245\",\"FMPC0177994335\",\"FMPC0177998875\",\"FMPC0178027192\",\"FMPP0079876537\",\"FMPP0079894043\",\"FMPP0080007406\",\"FMPP0080009417\",\"FMPP0080025550\",\"FMPP0080031453\",\"FMPP0080040392\",\"FMPP0080054989\",\"FMPP0080056248\",\"FMPP0080060687\",\"FMPP0080086680\",\"FMPP0080093725\"],\"system_weight\":{\"physical\":9070},\"seal_id\":\"BNA1861061-16\",\"serviceType\":\"REGULAR\",\"type\":\"bag\",\"current_location\":{\"id\":\"41\",\"type\":\"MOTHER_HUB\"},\"receiver_weight\":{},\"sender_weight\":{\"physical\":0},\"created_at\":\"2017-02-02T22:40:43.000+05:30\",\"source_location\":{\"id\":\"644\",\"type\":\"MOTHER_HUB\"},\"attributes\":[\"NonBook\",\"REGULAR\"],\"notes\":[]},\"test\":null,\"entityId\":\"Bag-29216101\",\"parentId\":null,\"entityVersion\":\"2017-02-03T23:06:32.416+05:30-CLOSED\",\"ingestedAt\":1486143393865,\"encodingType\":\"JSON\",\"seqId\":\"001486143392416000000\",\"parentVersion\":null,\"updatedAt\":1486143392416}}";
        ObjectMapper mapper = new ObjectMapper();
        //mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
        JsonNode jsonNode = mapper.readTree(payload);
        System.out.println(jsonNode.get("entity"));
    }
}
