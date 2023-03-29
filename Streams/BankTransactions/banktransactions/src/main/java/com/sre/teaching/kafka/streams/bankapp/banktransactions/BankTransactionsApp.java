package com.sre.teaching.kafka.streams.bankapp.banktransactions;


import com.sre.teaching.kafka.streams.bankapp.banktransactions.interfacing.KafkaAPIWrapper;

public class BankTransactionsApp {

    public static void main(String[] args) {

        System.out.println("Bank Balance App started");

        KafkaAPIWrapper kafkaAPIWrapper=new KafkaAPIWrapper(
                "bank-transactions-stream");

        try {

            kafkaAPIWrapper.SendAsyncMessages();

        } catch (InterruptedException e) {

            throw new RuntimeException(e);
        }

        kafkaAPIWrapper.Close();
    }

}
