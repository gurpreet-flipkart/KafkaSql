package org.kafka.grep.grepper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.kafka.grep.payloads.AddressEntity;
import org.kafka.grep.payloads.ShipmentGroupEntity;
import org.kafka.grep.payloads.ShipmentGroupPayload;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PathFinderTest {
    @Test
    public void getValue() throws Exception {
        ShipmentGroupPayload shipmentGroupPayload = new ShipmentGroupPayload();
        ShipmentGroupEntity data = new ShipmentGroupEntity();
        data.setCurrent_location(new AddressEntity("151204"));
        shipmentGroupPayload.setData(data);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(shipmentGroupPayload));
        String value = new PathFinder().getValue("data.current_location.id").apply(node).get(0);
        Assert.assertEquals("151204", value);
    }


    @Test
    public void getValue2() throws Exception {
        ShipmentGroupPayload shipmentGroupPayload = new ShipmentGroupPayload();
        ShipmentGroupEntity data = new ShipmentGroupEntity();
        data.setCurrent_location(new AddressEntity("151204"));
        shipmentGroupPayload.setData(data);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(shipmentGroupPayload));
        String value = new PathFinder().getValue("data.current_location.i").apply(node).get(0);
        Assert.assertEquals("null", value);
    }


    @Test
    public void testList() throws Exception {
        ArrayList<B> bList = new ArrayList<>();
        bList.add(new B("b1", ImmutableList.of()));
        bList.add(new B("b2", ImmutableList.of()));
        A a = new A("a", bList);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(a));
        List<String> list = new PathFinder().getValue("bl.b").apply(node);
        System.out.println(list);
        Assert.assertEquals("b1,b2", list.stream().collect(Collectors.joining(",")));
    }


    @Test
    public void testList2() throws Exception {
        ArrayList<B> bList = new ArrayList<>();
        bList.add(new B("b1", ImmutableList.of("s1", "s2")));
        bList.add(new B("b2", ImmutableList.of("s3", "s4")));
        A a = new A("a", bList);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(a));
        List<String> list = new PathFinder().getValue("bl.sl").apply(node);
        System.out.println(list);
        Assert.assertEquals("s1,s2,s3,s4", list.stream().collect(Collectors.joining(",")));
    }


}


@Data
@NoArgsConstructor
@AllArgsConstructor
class A {
    String a;
    List<B> bl;
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class B {
    String b;
    List<String> sl;
}