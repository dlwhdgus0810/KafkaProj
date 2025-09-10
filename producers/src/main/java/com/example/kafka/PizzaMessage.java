package com.example.kafka;


import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class PizzaMessage {
    // set pizza menu
    private static final List<String> pizzaNames = List.of("Potato Pizza", "Cheese Pizza",
            "Cheese Garlic Pizza", "Super Supreme", "Peperoni");

    // set pizza shop
    private static final List<String> pizzaShop = List.of("A001", "B001", "C001",
            "D001", "E001", "F001", "G001", "H001", "I001", "J001", "K001", "L001", "M001", "N001",
            "O001", "P001", "Q001");

    public PizzaMessage() {}

    // return random element from list
    private String getRandomValueFromList(List<String> list, Random random) {
        int size = list.size();
        int index = random.nextInt(size);

        return list.get(index);
    }

    // create random pizza order data
    public HashMap<String, String> produce_msg(Faker faker, Random random, int id) {

        String shopId = getRandomValueFromList(pizzaShop, random);
        String pizzaName = getRandomValueFromList(pizzaNames, random);

        String ordId = "ord"+id;
        String customerName = faker.name().fullName();
        String phoneNumber = faker.phoneNumber().phoneNumber();
        String address = faker.address().streetAddress();
        LocalDateTime now = LocalDateTime.now();
        String message = String.format("order_id:%s, shop:%s, pizza_name:%s, customer_name:%s, phone_number:%s, address:%s, time:%s"
                , ordId, shopId, pizzaName, customerName, phoneNumber, address
                , now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.KOREAN)));

        //System.out.println(message);
        HashMap<String, String> messageMap = new HashMap<>();
        messageMap.put("key", shopId);
        messageMap.put("message", message);

        return messageMap;
    }

    public static void main(String[] args) {
        PizzaMessage pizzaMessage = new PizzaMessage();

        long seed = 2022;
        Random random = new Random(seed);

        // create Faker object using Random(seed)
        Faker faker = Faker.instance(random);

        for(int i=0; i < 60; i++) {
            HashMap<String, String> message = pizzaMessage.produce_msg(faker, random, i);
            System.out.println("key:"+ message.get("key") + " message:" + message.get("message"));
        }

    }
}
