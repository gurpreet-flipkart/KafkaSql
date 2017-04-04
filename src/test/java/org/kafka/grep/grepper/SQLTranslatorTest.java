package org.kafka.grep.grepper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.kafka.grep.payloads.AddressEntity;
import org.kafka.grep.payloads.ShipmentGroupEntity;
import org.kafka.grep.payloads.ShipmentGroupPayload;
import org.kafka.grep.transformer.PredicateTransformer;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Predicate;

public class SQLTranslatorTest {
    @Test
    public void test() throws Exception {
        String expression = "select distinct x from shipmentGroupPayload where data.status='EXPECTED' and (data.current_location.id='151204' or entityId='Yo')";
        SQLParser parser = new SQLParser();
        ShipmentGroupPayload shipmentGroupPayload = new ShipmentGroupPayload();
        ShipmentGroupEntity data = new ShipmentGroupEntity();
        data.setCurrent_location(new AddressEntity("151204"));
        shipmentGroupPayload.setEntityId("Yo");
        data.setStatus("EXPECTED");
        shipmentGroupPayload.setEntityId("Bag-1");
        shipmentGroupPayload.setData(data);
        Optional<SqlSelect> sqlSelect = parser.getSqlSelect(expression);
        Predicate parse = new PredicateTransformer().conditionalClause(sqlSelect.orElseThrow(() -> new IllegalArgumentException()));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(shipmentGroupPayload));
        boolean test = parse.test(node);
        Assert.assertTrue(test);
    }


    @Test
    public void testNull() throws Exception {
        String expression = "select  x from shipmentGroupPayload where (data.current_location.id is null)";
        SQLParser parser = new SQLParser();
        ShipmentGroupPayload shipmentGroupPayload = new ShipmentGroupPayload();
        ShipmentGroupEntity data = new ShipmentGroupEntity();
        data.setCurrent_location(new AddressEntity(null));
        shipmentGroupPayload.setData(data);
        Optional<SqlSelect> sqlSelect = parser.getSqlSelect(expression);
        Predicate parse = new PredicateTransformer().conditionalClause(sqlSelect.orElseThrow(() -> new IllegalArgumentException()));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(shipmentGroupPayload));
        boolean test = parse.test(node);
        Assert.assertTrue(test);
    }

    @Test
    public void testNull2() throws Exception {
        String expression = "select  x from shipmentGroupPayload where (data.current_location.id is not null)";
        SQLParser parser = new SQLParser();
        ShipmentGroupPayload shipmentGroupPayload = new ShipmentGroupPayload();
        ShipmentGroupEntity data = new ShipmentGroupEntity();
        data.setCurrent_location(new AddressEntity("100"));
        shipmentGroupPayload.setData(data);
        Optional<SqlSelect> sqlSelect = parser.getSqlSelect(expression);
        Predicate parse = new PredicateTransformer().conditionalClause(sqlSelect.orElseThrow(() -> new IllegalArgumentException()));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(shipmentGroupPayload));
        boolean test = parse.test(node);
        Assert.assertTrue(test);
    }

    @Test
    public void testNull3() throws Exception {
        String expression = "select  x from shipmentGroupPayload where (data.current_location is null)";
        SQLParser parser = new SQLParser();
        ShipmentGroupPayload shipmentGroupPayload = new ShipmentGroupPayload();
        ShipmentGroupEntity data = new ShipmentGroupEntity();
        data.setCurrent_location(null);
        shipmentGroupPayload.setData(data);
        Optional<SqlSelect> sqlSelect = parser.getSqlSelect(expression);
        Predicate parse = new PredicateTransformer().conditionalClause(sqlSelect.orElseThrow(() -> new IllegalArgumentException()));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree("{\"entityId\":\"-1\",\"updatedAt\":1487818653075,\"data\":{\"updated_at\":1487818653075,\"shipping_updated_at\":-1,\"erp_updated_at\":-1, \"vendor_tracking_id\":\"FMPC0174249294\",\"agent_id\":-1,\"runsheet_id\":-1,\"attributes\":[],\"shipment_attributes\":{},\"consignment\":{\"id\":20723707,\"updated_at\":1487818653075,\"hub_id\":null,\"source_hub_id\":null,\"destination_hub_id\":null,\"status\":null},\"fm_updated_at\":-1,\"shipment_items\":[],\"cs_action_flag\":-1,\"created_at\":-1}}");
        boolean test = parse.test(node);
        Assert.assertTrue(test);
    }


    @Test
    public void test2() throws Exception {
        String expression = "select distinct x from shipmentGroupPayload where entityId='Bag-1'";
        SQLParser parser = new SQLParser();
        ShipmentGroupPayload shipmentGroupPayload = new ShipmentGroupPayload();
        ShipmentGroupEntity data = new ShipmentGroupEntity();
        shipmentGroupPayload.setEntityId("Bag-1");
        shipmentGroupPayload.setData(data);
        Optional<SqlSelect> sqlSelect = parser.getSqlSelect(expression);
        Predicate parse = new PredicateTransformer().conditionalClause(sqlSelect.orElseThrow(() -> new IllegalArgumentException()));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(shipmentGroupPayload));
        boolean test = parse.test(node);
        Assert.assertTrue(test);
    }


    @Test
    public void test3() throws Exception {
        String expression = "select  x from shipmentGroupPayload";
        SQLParser parser = new SQLParser();
        ShipmentGroupPayload shipmentGroupPayload = new ShipmentGroupPayload();
        ShipmentGroupEntity data = new ShipmentGroupEntity();
        shipmentGroupPayload.setData(data);
        Optional<SqlSelect> sqlSelect = parser.getSqlSelect(expression);
        Predicate parse = new PredicateTransformer().conditionalClause(sqlSelect.orElseThrow(() -> new IllegalArgumentException()));
        Assert.assertNull(parse);
    }


    @Test
    public void testListEmpty() throws Exception {
        String expression = "select  x from shipmentGroupPayload where bl.sl is null";
        ArrayList<B> bList = new ArrayList<>();
        bList.add(new B("b1", ImmutableList.of()));
        A a = new A("a", bList);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(a));
        SQLParser parser = new SQLParser();
        Optional<SqlSelect> sqlSelect = parser.getSqlSelect(expression);
        //Assert.assertEquals("s1,s2,s3,s4", list.stream().collect(Collectors.joining(",")));
        Predicate parse = new PredicateTransformer().conditionalClause(sqlSelect.orElseThrow(() -> new IllegalArgumentException()));
        boolean test = parse.test(node);
        Assert.assertTrue(test);

    }

    @Test
    public void testListNull() throws Exception {
        String expression = "select  x from shipmentGroupPayload where bl.sl is null";
        ArrayList<B> bList = new ArrayList<>();
        bList.add(new B("b1", null));
        A a = new A("a", bList);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(a));
        SQLParser parser = new SQLParser();
        Optional<SqlSelect> sqlSelect = parser.getSqlSelect(expression);
        //Assert.assertEquals("s1,s2,s3,s4", list.stream().collect(Collectors.joining(",")));
        Predicate parse = new PredicateTransformer().conditionalClause(sqlSelect.orElseThrow(() -> new IllegalArgumentException()));
        boolean test = parse.test(node);
        Assert.assertTrue(test);

    }


}