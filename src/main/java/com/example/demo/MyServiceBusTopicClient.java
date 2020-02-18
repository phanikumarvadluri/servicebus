package com.example.demo;

import com.google.gson.reflect.TypeToken;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.google.gson.Gson;
import static java.nio.charset.StandardCharsets.*;
import java.time.Duration;
import java.util.*;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MyServiceBusTopicClient {

	static final Gson GSON = new Gson();

	public static void main(String[] args) throws Exception, ServiceBusException {
		// TODO Auto-generated method stub

		TopicClient sendClient;
		String connectionString = "Endpoint=sb://topicdemosra1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=4shWeZo0myVaEfC7B31gFpq/5cZT/vYtZmzAyhwL4gc=";
		sendClient = new TopicClient(new ConnectionStringBuilder(connectionString, "BasicTopic"));
		sendMessagesAsync(sendClient).thenRunAsync(() -> sendClient.closeAsync());
	}

	static CompletableFuture<Void> sendMessagesAsync(TopicClient sendClient) {
		List<HashMap<String, String>> data = GSON.fromJson("[" + "{'name' = 'Einstein', 'firstName' = 'Albert'},"
				+ "{'name' = 'Heisenberg', 'firstName' = 'Werner'}," + "{'name' = 'Curie', 'firstName' = 'Marie'},"
				+ "{'name' = 'Hawking', 'firstName' = 'Steven'}," + "{'name' = 'Newton', 'firstName' = 'Isaac'},"
				+ "{'name' = 'Bohr', 'firstName' = 'Niels'}," + "{'name' = 'Faraday', 'firstName' = 'Michael'},"
				+ "{'name' = 'Galilei', 'firstName' = 'Galileo'}," + "{'name' = 'Kepler', 'firstName' = 'Johannes'},"
				+ "{'name' = 'Kopernikus', 'firstName' = 'Nikolaus'}" + "]",
				new TypeToken<List<HashMap<String, String>>>() {
				}.getType());

		List<CompletableFuture> tasks = new ArrayList<>();
		for (int i = 0; i < data.size(); i++) {
			final String messageId = Integer.toString(i);
			Message message = new Message(GSON.toJson(data.get(i), Map.class).getBytes(UTF_8));
			message.setContentType("application/json");
			message.setLabel("Scientist");
			message.setMessageId(messageId);
			message.setTimeToLive(Duration.ofMinutes(2));
			System.out.printf("Message sending: Id = %s\n", message.getMessageId());
			tasks.add(sendClient.sendAsync(message).thenRunAsync(() -> {
				System.out.printf("\tMessage acknowledged: Id = %s\n", message.getMessageId());
			}));
		}
		return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[tasks.size()]));
	}

}
