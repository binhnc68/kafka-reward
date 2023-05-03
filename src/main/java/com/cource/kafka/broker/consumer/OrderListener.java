package com.cource.kafka.broker.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.cource.kafka.message.OrderMessage;

@Service
public class OrderListener {

	private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

	@KafkaListener(topics = "t.commodity.order")
	public void listen(ConsumerRecord<String, OrderMessage> consumerRecord) {
		Headers headers = consumerRecord.headers();
		OrderMessage orderMessage = consumerRecord.value();

		log.info("OrderListener.listen - order {} ,item {} , credit card number {} ", orderMessage.getOrderNumber(),
				orderMessage.getItemName(), orderMessage.getCreditCardNumber());

		for (Header header : headers) {
			log.info("OrderListener.listen - key: " + header.key() + ",value:" + header.value());
		}

	}
}
