package io.rcardin.spring.kafka.stream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public MessageConverter jackson2JsonMessageConverter() {
		return new Jackson2JsonMessageConverter();
	}

	@Bean
	public Queue wordsQueue() {
		return new Queue("words", true);
	}

	@Bean
	public Queue wordCountersQueue() {
		return new Queue("word-counters", true);
	}

	@RabbitListener(queues = "words")
	public void processWord(@Payload String word) {
		String[] words = word.split(" ");
		for (String w : words) {
			sendWordCount(w);
		}
	}

	private void sendWordCount(String word) {
		// Simulate counting logic
		AtomicLong count = new AtomicLong(0);
		count.incrementAndGet();
		// In a real scenario, this would send to RabbitMQ
	}

	@Component
	public static class WordCounterProcessor {

		@Bean
		@ServiceActivator(inputChannel = "words")
		public void processWordStream(String word) {
			String[] words = word.split(" ");
			for (String w : words) {
				sendWordCount(w);
			}
		}

		private void sendWordCount(String word) {
			// Simulate counting logic
			AtomicLong count = new AtomicLong(0);
			count.incrementAndGet();
			// In a real scenario, this would send to RabbitMQ
		}
	}
}