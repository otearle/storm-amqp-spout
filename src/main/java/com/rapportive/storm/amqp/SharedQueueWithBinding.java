package com.rapportive.storm.amqp;

import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Declares a named, durable queue and binds it to an existing exchange.  This
 * is a good choice for production use as the queue will survive spout
 * restarts, so you won't miss messages if your spout crashes.
 *
 * <p><strong>N.B.</strong> this could be risky under some circumstances. e.g.
 * if while prototyping you set a development topology consuming from a
 * production AMQP server, then kill your topology and go home for the night;
 * messages will continue to be queued up, which could threaten the stability
 * of the AMQP server if the exchange is high-volume.  For prototyping consider
 * {@link ExclusiveQueueWithBinding}.</p>
 *
 * <p>This queue is safe for multiple parallel spout tasks: as they all consume
 * the same named queue, the AMQP broker will round-robin messages between
 * them, so each message will get processed only once (barring redelivery due
 * to outages).</p>
 */
public class SharedQueueWithBinding implements QueueDeclaration {
	private static final long serialVersionUID = 2364833412534518859L;

	private final String queueName;
	private final String exchange;
	private final String routingKey;
	private long queue_ttl, queue_expires, queue_max_length = -1;

	/**
	 * Create a declaration of a named, durable, non-exclusive queue bound to
	 * the specified exchange.
	 *
	 * @param queueName  name of the queue to be declared.
	 * @param exchange  exchange to bind the queue to.
	 * @param routingKey  routing key for the exchange binding.  Use "#" to
	 *                    receive all messages published to the exchange.
     * @param queue_ttl time (in milliseconds) for each message on the queue to
     *                  be considered "alive" http://www.rabbitmq.com/ttl.html
     * @param queue_expires time (in milliseconds) for the queue to stick around
     *                      without a consumer. http://www.rabbitmq.com/ttl.html
     * @param queue_max_length maximum number of messages to allow on the queue.
     *                         This includes acked and unacked messages.  Further
     *                         messages delivered to the queue will be dropped or
     *                         sent to the dead-letter queue
	 */
	public SharedQueueWithBinding(String queueName, String exchange, String
			routingKey, long queue_ttl, long queue_expires, long
			queue_max_length) {
		this.queueName = queueName;
		this.exchange = exchange;
		this.routingKey = routingKey;
		this.queue_ttl = queue_ttl;
		this.queue_expires = queue_expires;
		this.queue_max_length = queue_max_length;
	}

    /**
     * Create a declaration of a named, durable, non-exclusive queue bound to
     * the specified exchange.
     *
     * @param queueName  name of the queue to be declared.
     * @param exchange  exchange to bind the queue to.
     * @param routingKey  routing key for the exchange binding.  Use "#" to
     *                    receive all messages published to the exchange.
     */
    public SharedQueueWithBinding(String queueName, String exchange, String routingKey) {
        this.queueName = queueName;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

	/**
	 * Verifies the exchange exists, creates the named queue if it does not
	 * exist, and binds it to the exchange.
	 *
	 * @return the server's response to the successful queue declaration.
	 *
	 * @throws IOException  if the exchange does not exist, the queue could not
	 *                      be declared, or if the AMQP connection drops.
	 */
	@Override
	public Queue.DeclareOk declare(Channel channel) throws IOException {
        final Queue.DeclareOk queue;
        Queue.DeclareOk queue1;
        Map<String, Object> args = new HashMap<String, Object>();
        if (queue_ttl > -1) args.put("x-message-ttl", queue_ttl);
		if (queue_expires > -1) args.put("x-expires",     queue_expires);
		if (queue_max_length > -1) args.put("x-max-length",  queue_max_length);
        try {
            channel.exchangeDeclarePassive(exchange);
        } catch (IOException e) {
            channel.exchangeDeclare(exchange, "direct", true);
        }
        try {
           queue1 = channel.queueDeclarePassive(queueName);
        } catch (IOException e) {
            // The args for queueDeclare are: name, durable, exclusive, autoDelete, arguments
            queue1 = channel.queueDeclare(queueName,true,false,false,args);
        }
        queue = queue1;
        channel.queueBind(queue.getQueue(), exchange, routingKey);

		return queue;
	}

	/**
	 * Returns <tt>true</tt> as this queue is safe for parallel consumers.
	 */
	@Override
	public boolean isParallelConsumable() {
		return true;
	}
}
