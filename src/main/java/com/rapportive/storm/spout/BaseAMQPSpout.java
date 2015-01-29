package com.rapportive.storm.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.*;
import com.rapportive.storm.amqp.QueueDeclaration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

/**
 * Spout to feed messages into Storm from an AMQP queue.  Each message routed
 * to the queue will be emitted as a Storm tuple.  The message will be acked or
 * rejected once the topology has respectively fully processed or failed the
 * corresponding tuple.
 *
 * <p><strong>N.B.</strong> if you need to guarantee all messages are reliably
 * processed, you should have AMQPSpout consume from a queue that is
 * <em>not</em> set as 'exclusive' or 'auto-delete': otherwise if the spout
 * task crashes or is restarted, the queue will be deleted and any messages in
 * it lost, as will any messages published while the task remains down.  See
 * {@link com.rapportive.storm.amqp.SharedQueueWithBinding} to declare a shared
 * queue that allows for guaranteed processing.  (For prototyping, an
 * {@link com.rapportive.storm.amqp.ExclusiveQueueWithBinding} may be
 * simpler to manage.)</p>
 *
 * <p><strong>N.B.</strong> this does not currently handle malformed messages
 * (which cannot be deserialised by the provided {@link Scheme}) very well:
 * the spout worker will crash if it fails to serialise a message.</p>
 *
 * <p>This consumes messages from AMQP asynchronously, so it may receive
 * messages before Storm requests them as tuples; therefore it buffers messages
 * in an internal queue.  To avoid this buffer growing large and consuming too
 * much RAM, set {@link #CONFIG_PREFETCH_COUNT}.</p>
 *
 * <p>This spout can be distributed among multiple workers, depending on the
 * queue declaration: see {@link QueueDeclaration#isParallelConsumable}.</p>
 *
 * @see QueueDeclaration
 * @see com.rapportive.storm.amqp.SharedQueueWithBinding
 * @see com.rapportive.storm.amqp.ExclusiveQueueWithBinding
 *
 * @author Sam Stokes (sam@rapportive.com)
 */
public abstract class BaseAMQPSpout extends BaseRichSpout {
    private static final long serialVersionUID = 11258942292629264L;

    protected static final Logger log = Logger.getLogger(BaseAMQPSpout.class);

    /**
     * Storm config key to set the AMQP basic.qos prefetch-count parameter.
     * Defaults to 100.
     *
     * <p>This caps the number of messages outstanding (i.e. unacked) at a time
     * that will be sent to each spout worker.  Increasing this will improve
     * throughput if the network roundtrip time to the AMQP broker is
     * significant compared to the time for the topology to process each
     * message; this will also increase the RAM requirements as the internal
     * message buffer grows.</p>
     *
     * <p>AMQP allows a prefetch-count of zero, indicating unlimited delivery,
     * but that is not allowed here to avoid unbounded buffer growth.</p>
     */
    public static final String CONFIG_PREFETCH_COUNT = "amqp.prefetch.count";
    private static final long DEFAULT_PREFETCH_COUNT = 100;

    /**
     * Time in milliseconds to wait for a message from the queue if there is
     * no message ready when the topology requests a tuple (via
     * {@link #nextTuple()}).
     */
    public static final long WAIT_FOR_NEXT_MESSAGE = 1L;

    /**
     * Time in milliseconds to wait after losing connection to the AMQP broker
     * before attempting to reconnect.
     */
    public static final long WAIT_AFTER_SHUTDOWN_SIGNAL = 10000L;

    /**
     * Name of the stream where malformed deserialized messages are sent for
     * special handling. Generally with a {@link Scheme} implementation returns
     * null or a zero-length tuple.  `enableErrorStream` must be true at
     * construction.
     */
    public static String ERROR_STREAM_NAME = "error-stream";

    protected final String amqpHost;
    protected final int amqpPort;
    protected final String amqpUsername;
    protected final String amqpPassword;
    protected final String amqpVhost;
    protected final boolean requeueOnFail;
    protected final boolean enableErrorStream;
    protected final boolean autoAck;

    protected final QueueDeclaration queueDeclaration;

    protected final Scheme serialisationScheme;

    protected boolean spoutActive = true;
    private transient Connection amqpConnection;
    protected transient Channel amqpChannel;
    protected transient QueueingConsumer amqpConsumer;
    private transient String amqpConsumerTag;

    protected SpoutOutputCollector collector;

    private int prefetchCount;
    protected TreeMap<UUID,Long> messageIdMap;



    /**
     * Create a new AMQP spout.  When
     * {@link #open(Map, TopologyContext, SpoutOutputCollector)} is called, it
     * will declare a queue according to the specified
     * <tt>queueDeclaration</tt>, subscribe to the queue, and start consuming
     * messages.  It will use the provided <tt>scheme</tt> to deserialise each
     * AMQP message into a Storm tuple. Note that failed messages will not be
     * requeued.
     *
     * @param host  hostname of the AMQP broker node
     * @param port  port number of the AMQP broker node
     * @param username  username to log into to the broker
     * @param password  password to authenticate to the broker
     * @param vhost  vhost on the broker
     * @param queueDeclaration  declaration of the queue / exchange bindings
     * @param scheme  {@link backtype.storm.spout.Scheme} used to deserialise
     *          each AMQP message into a Storm tuple
     */
    public BaseAMQPSpout(String host, int port, String username, String password,
                         String vhost, QueueDeclaration queueDeclaration, Scheme scheme) {
        this(host, port, username, password, vhost, queueDeclaration, scheme, false, true, false);
    }

    /**
     * Create a new AMQP spout.  When
     * {@link #open(Map, TopologyContext, SpoutOutputCollector)} is called, it
     * will declare a queue according to the specified
     * <tt>queueDeclaration</tt>, subscribe to the queue, and start consuming
     * messages.  It will use the provided <tt>scheme</tt> to deserialise each
     * AMQP message into a Storm tuple.
     *
     * @param host  hostname of the AMQP broker node
     * @param port  port number of the AMQP broker node
     * @param username  username to log into to the broker
     * @param password  password to authenticate to the broker
     * @param vhost  vhost on the broker
     * @param queueDeclaration  declaration of the queue / exchange bindings
     * @param scheme  {@link backtype.storm.spout.Scheme} used to deserialise
     *          each AMQP message into a Storm tuple
     * @param requeueOnFail  whether messages should be requeued on failure
     * @param enableErrorStream  emit error stream
     */
    public BaseAMQPSpout(String host, int port, String username, String password,
                         String vhost, QueueDeclaration queueDeclaration, Scheme scheme,
                         boolean requeueOnFail, boolean enableErrorStream, boolean autoAck) {
        this.amqpHost = host;
        this.amqpPort = port;
        this.amqpUsername = username;
        this.amqpPassword = password;
        this.amqpVhost = vhost;
        this.queueDeclaration = queueDeclaration;
        this.requeueOnFail = requeueOnFail;
        this.enableErrorStream = enableErrorStream;
        this.autoAck = autoAck;

        this.serialisationScheme = scheme;
    }


    /**
     * Emits the next message from the queue as a tuple.
     *
     * Serialization schemes returning null will immediately ack
     * and then emit unanchored on the {@link #ERROR_STREAM_NAME} stream for
     * further handling by the consumer.
     *
     * <p>If no message is ready to emit, this will wait a short time
     * ({@link #WAIT_FOR_NEXT_MESSAGE}) for one to arrive on the queue,
     * to avoid a tight loop in the spout worker.</p>
     */
    @Override
    public void nextTuple() {
        if (spoutActive && amqpConsumer != null) {
            try {
                final QueueingConsumer.Delivery delivery = amqpConsumer.nextDelivery(WAIT_FOR_NEXT_MESSAGE);
                if (delivery == null) return;
                // add the deliveryTag to our list of tuples we care about
                UUID uniqueId = UUID.randomUUID();
                messageIdMap.put(uniqueId, (Long) delivery.getEnvelope().getDeliveryTag());
                handleOneMessage(delivery, uniqueId);
            } catch (ShutdownSignalException e) {
                log.warn("AMQP connection dropped, will attempt to reconnect...");
                Utils.sleep(WAIT_AFTER_SHUTDOWN_SIGNAL);
                this.reconnect();
            } catch (ConsumerCancelledException e) {
                log.warn("AMQP consumer cancelled, will attempt to reconnect...");
                Utils.sleep(WAIT_AFTER_SHUTDOWN_SIGNAL);
                this.reconnect();
            } catch (InterruptedException e) {
                // interrupted while waiting for message, big deal
            }
        }
    }

    /**
     * Deals with one non-null message at a time
     * In the basic spout, this serializes and emits a message.
     * To change the way messages are handled, override this method
     *
     * @param delivery The thing delivered by amqp which has a body, properties, and a deliveryTag
     * @param uniqueId A unique reference that storm can use to send notifications back up the topology  Probably an instance of UUID
     */
    public void handleOneMessage(QueueingConsumer.Delivery delivery, Object uniqueId) {
        final byte[] message = delivery.getBody();
        List<Object> deserializedMessage = serialisationScheme.deserialize(message);
        if (deserializedMessage != null && deserializedMessage.size() > 0) {
            collector.emit(deserializedMessage, uniqueId);
        } else {
            handleMalformedDelivery(delivery, uniqueId);
        }
    }


    /**
     * Acks the message with the AMQP broker and includes protection for
     * when the msgID is no longer valid on our existing channel
     * @param msgId AMQP deliveryTag
     */
    @Override
    public void ack(Object msgId) {
        // This only works if we still have the same AMQP connection
        // So we have to go through this nonsense to prevent the connection dropping
        if (msgId instanceof UUID) {
            final UUID msgUUID = (UUID) msgId;
            Object deliveryTagObj = messageIdMap.get(msgUUID);
            if (deliveryTagObj instanceof Long) {
                final Long deliveryTag = (Long) deliveryTagObj;
                if (amqpChannel != null) {
                    try {
                        if (!autoAck && amqpChannel.isOpen()) {
                            amqpChannel.basicAck(deliveryTag, false /* not multiple */);
                        }
                        messageIdMap.remove(msgUUID);
                    } catch (IOException e) {
                        log.warn("Failed to ack delivery-tag " + deliveryTag, e);
                    } catch (ShutdownSignalException e) {
                        log.warn("AMQP connection failed. Failed to ack delivery-tag " + deliveryTag, e);
                    }
                } else log.warn(String.format("Cannot reject message %s on a closed channel.",  msgUUID.toString()));
            } else log.warn(String.format("Cannot reject unknown message %s.  This channel knows about %d messages.", msgUUID.toString(), messageIdMap.size()));
        }
    }

    public void handleMalformedDelivery(QueueingConsumer.Delivery delivery, Object msgId){
        log.debug(String.format("Rejecting Malformed Message from AMQP"));
        if (!this.autoAck) {
            ack(msgId);
        }
        if (enableErrorStream) {
            collector.emit(ERROR_STREAM_NAME, new Values(delivery.getEnvelope().getDeliveryTag(), delivery));
        }
    }

    /**
     * Cancels the queue subscription, and disconnects from the AMQP broker.
     */
    @Override
    public void close() {
        try {
            if (amqpChannel != null) {
              if (amqpConsumerTag != null) {
                  amqpChannel.basicCancel(amqpConsumerTag);
              }

              amqpChannel.close();
            }
        } catch (IOException e) {
            log.warn("Error closing AMQP channel", e);
        }

        try {
            if (amqpConnection != null) {
              amqpConnection.close();
            }
        } catch (IOException e) {
            log.warn("Error closing AMQP connection", e);
        }
    }

    /**
     * Resumes a paused spout
     */
    public void activate() {
        log.info("Unpausing spout");
        if (amqpChannel == null) {reconnect();}
        if (!this.amqpChannel.isOpen()) {reconnect();}
        spoutActive = true;
    }

    /**
     * Pauses the spout
     */
    public void deactivate() {
        log.info("Pausing spout");
        spoutActive = false;
    }

    /**
     * Tells the AMQP broker to drop (Basic.Reject) the message.
     *
     * requeueOnFail constructor parameter determines whether the message will be requeued.
     *
     * <p><strong>N.B.</strong> There's a potential for infinite
     * redelivery in the event of non-transient failures (e.g. malformed
     * messages).
     *
     */
    @Override
    public void fail(Object msgId) {
        // This only works if we still have the same AMQP connection
        // So we have to go through this nonsense to prevent the connection dropping
        if (msgId instanceof UUID) {
            final UUID msgUUID = (UUID) msgId;
            Object deliveryTagObj = messageIdMap.get(msgUUID);
            if (deliveryTagObj instanceof Long) {
                final Long deliveryTag = (Long) deliveryTagObj;
                if (amqpChannel != null) {
                    try {
                        if (!autoAck) {
                            amqpChannel.basicReject(deliveryTag, requeueOnFail);
                        }
                        messageIdMap.remove(msgUUID);
                    } catch (IOException e) {
                        log.warn("Failed to reject delivery-tag " + deliveryTag, e);
                    } catch (ShutdownSignalException e) {
                        log.warn("AMQP connection failed. Failed to reject delivery-tag " + deliveryTag, e);
                    }
                } else log.warn(String.format("Cannot reject message %s on a closed channel.",  msgUUID.toString()));
            } else log.warn(String.format("Cannot reject unknown message %s.  This channel knows about %d messages.", msgUUID.toString(), messageIdMap.size()));
        }
    }

    private void setupAMQP() throws IOException {
        final int prefetchCount = this.prefetchCount;

        final ConnectionFactory connectionFactory = new ConnectionFactory() {
            public void configureSocket(Socket socket)
                throws IOException {
                socket.setTcpNoDelay(false);
                socket.setReceiveBufferSize(20*1024);
                socket.setSendBufferSize(20*1024);
            }
        };

        connectionFactory.setHost(amqpHost);
        connectionFactory.setPort(amqpPort);
        connectionFactory.setUsername(amqpUsername);
        connectionFactory.setPassword(amqpPassword);
        connectionFactory.setVirtualHost(amqpVhost);

        this.amqpConnection = connectionFactory.newConnection();
        this.amqpChannel = amqpConnection.createChannel();

        log.info("Setting basic.qos prefetch-count to " + prefetchCount);
        amqpChannel.basicQos(prefetchCount);

        final Queue.DeclareOk queue = queueDeclaration.declare(amqpChannel);
        final String queueName = queue.getQueue();
        log.info("Consuming queue " + queueName + "with autoAck=" + this.autoAck);

        this.amqpConsumer = new QueueingConsumer(amqpChannel);
        assert this.amqpConsumer != null;
        this.amqpConsumerTag = amqpChannel.basicConsume(queueName, this.autoAck, amqpConsumer);
    }

    protected void reconnect() {
        log.info("Reconnecting to AMQP broker...");
        try {
        	if (messageIdMap != null) {
        		messageIdMap.clear();
        	}
            setupAMQP();
        } catch (IOException e) {
            log.warn("Failed to reconnect to AMQP broker", e);
        }
    }

    /**
     * Connects to the AMQP broker, declares the queue and subscribes to
     * incoming messages.
     */
    @Override
    public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
        Long prefetchCount = (Long) config.get(CONFIG_PREFETCH_COUNT);
        if (prefetchCount == null) {
            log.info("Using default prefetch-count");
            prefetchCount = DEFAULT_PREFETCH_COUNT;
        } else if (prefetchCount < 1) {
            throw new IllegalArgumentException(CONFIG_PREFETCH_COUNT + " must be at least 1");
        }
        this.prefetchCount = prefetchCount.intValue();

        try {
            this.collector = collector;
            // Set up our map of message ids that we are tracking
            messageIdMap = new TreeMap<UUID,Long>();
            setupAMQP();

        } catch (IOException e) {
            log.error("AMQP setup failed", e);
            log.warn("AMQP setup failed, will attempt to reconnect...");
            Utils.sleep(WAIT_AFTER_SHUTDOWN_SIGNAL);
            reconnect();
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
