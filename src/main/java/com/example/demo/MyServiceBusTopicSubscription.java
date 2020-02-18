package com.example.demo;

import com.google.gson.reflect.TypeToken;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.google.gson.Gson;
import static java.nio.charset.StandardCharsets.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import org.apache.commons.cli.*;
import org.apache.commons.cli.DefaultParser;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.google.gson.Gson;
import com.microsoft.azure.servicebus.ExceptionPhase;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageHandler;
import com.microsoft.azure.servicebus.MessageHandlerOptions;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.SubscriptionClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public class MyServiceBusTopicSubscription {

	static final Gson GSON = new Gson();

	public static void main(String[] args) throws Exception, ServiceBusException {
		String connectionString = "Endpoint=sb://topicdemosra1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=4shWeZo0myVaEfC7B31gFpq/5cZT/vYtZmzAyhwL4gc=";
		SubscriptionClient subscription1Client = new SubscriptionClient(
				new ConnectionStringBuilder(connectionString, "BasicTopic/subscriptions/Subscription1"),
				ReceiveMode.PEEKLOCK);
		SubscriptionClient subscription2Client = new SubscriptionClient(
				new ConnectionStringBuilder(connectionString, "BasicTopic/subscriptions/Subscription2"),
				ReceiveMode.PEEKLOCK);
		SubscriptionClient subscription3Client = new SubscriptionClient(
				new ConnectionStringBuilder(connectionString, "BasicTopic/subscriptions/Subscription3"),
				ReceiveMode.PEEKLOCK);

		registerMessageHandlerOnClient(subscription1Client);
		registerMessageHandlerOnClient(subscription2Client);
		registerMessageHandlerOnClient(subscription3Client);
	}

	static void registerMessageHandlerOnClient(SubscriptionClient receiveClient) throws Exception {

		// register the RegisterMessageHandler callback
		IMessageHandler messageHandler = new IMessageHandler() {
			// callback invoked when the message handler loop has obtained a
			// message
			public CompletableFuture<Void> onMessageAsync(IMessage message) {
				// receives message is passed to callback
				if (message.getLabel() != null && message.getContentType() != null
						&& message.getLabel().contentEquals("Scientist")
						&& message.getContentType().contentEquals("application/json")) {

					byte[] body = message.getBody();
					Map scientist = GSON.fromJson(new String(body, UTF_8), Map.class);

					System.out.printf(
							"\n\t\t\t\t%s Message received: \n\t\t\t\t\t\tMessageId = %s, \n\t\t\t\t\t\tSequenceNumber = %s, \n\t\t\t\t\t\tEnqueuedTimeUtc = %s,"
									+ "\n\t\t\t\t\t\tExpiresAtUtc = %s, \n\t\t\t\t\t\tContentType = \"%s\",  \n\t\t\t\t\t\tContent: [ firstName = %s, name = %s ]\n",
							receiveClient.getEntityPath(), message.getMessageId(), message.getSequenceNumber(),
							message.getEnqueuedTimeUtc(), message.getExpiresAtUtc(), message.getContentType(),
							scientist != null ? scientist.get("firstName") : "",
							scientist != null ? scientist.get("name") : "");
				}
				return receiveClient.completeAsync(message.getLockToken());
			}

			public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
				System.out.printf(exceptionPhase + "-" + throwable.getMessage());
			}
		};

		receiveClient.registerMessageHandler(messageHandler,
				// callback invoked when the message handler has an exception to
				// report
				// 1 concurrent call, messages are auto-completed, auto-renew
				// duration
				new MessageHandlerOptions(1, false, Duration.ofMinutes(1)));

	}

}
