package com.rapportive.storm.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rapportive.storm.amqp.QueueDeclaration;

import java.util.List;

/**
 * Created by alt on 6/2/14.
 */
public class AMQPSpout extends BaseAMQPSpout{
    /**
     * Create a new AMQP spout.  When
     * {@link #open(Map, TopologyContext, SpoutOutputCollector)} is called, it
     * will declare a queue according to the specified
     * <tt>queueDeclaration</tt>, subscribe to the queue, and start consuming
     * messages.  It will use the provided <tt>scheme</tt> to deserialise each
     * AMQP message into a Storm tuple. Note that failed messages will not be
     * requeued.
     *
     * @param host             hostname of the AMQP broker node
     * @param port             port number of the AMQP broker node
     * @param username         username to log into to the broker
     * @param password         password to authenticate to the broker
     * @param vhost            vhost on the broker
     * @param queueDeclaration declaration of the queue / exchange bindings
     * @param scheme           {@link backtype.storm.spout.Scheme} used to deserialise
     */
    public AMQPSpout(String host, int port, String username, String password, String vhost, QueueDeclaration queueDeclaration, Scheme scheme) {
        super(host, port, username, password, vhost, queueDeclaration, scheme);
    }

    /**
     * Create a new AMQP spout.  When
     * {@link #open(Map, TopologyContext, SpoutOutputCollector)} is called, it
     * will declare a queue according to the specified
     * <tt>queueDeclaration</tt>, subscribe to the queue, and start consuming
     * messages.  It will use the provided <tt>scheme</tt> to deserialise each
     * AMQP message into a Storm tuple.
     *
     * @param host              hostname of the AMQP broker node
     * @param port              port number of the AMQP broker node
     * @param username          username to log into to the broker
     * @param password          password to authenticate to the broker
     * @param vhost             vhost on the broker
     * @param queueDeclaration  declaration of the queue / exchange bindings
     * @param scheme            {@link backtype.storm.spout.Scheme} used to deserialise
     *                          each AMQP message into a Storm tuple
     * @param requeueOnFail     whether messages should be requeued on failure
     * @param enableErrorStream emit error stream
     * @param autoAck
     */
    public AMQPSpout(String host, int port, String username, String password, String vhost, QueueDeclaration queueDeclaration, Scheme scheme, boolean requeueOnFail, boolean enableErrorStream, boolean autoAck) {
        super(host, port, username, password, vhost, queueDeclaration, scheme, requeueOnFail, enableErrorStream, autoAck);
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
                final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                final byte[] message = delivery.getBody();

                List<Object> deserializedMessage = serialisationScheme.deserialize(message);

                if (deserializedMessage != null && deserializedMessage.size() > 0) {
                    collector.emit(deserializedMessage, deliveryTag);
                } else {
                    handleMalformedDelivery(deliveryTag, message);
                }
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
     * Acks the bad message to avoid retry loops. Also emits the bad message
     * unreliably on the {@link #ERROR_STREAM_NAME} stream for consumer handling.
     * @param deliveryTag AMQP delivery tag
     * @param message bytes of the bad message
     */
    protected void handleMalformedDelivery(long deliveryTag, byte[] message) {
        log.debug("Malformed deserialized message, null or zero-length. " + deliveryTag);
        if (!this.autoAck) {
            ack(deliveryTag);
        }
        if (enableErrorStream) {
            collector.emit(ERROR_STREAM_NAME, new Values(deliveryTag, message));
        }
    }

    /**
     * Declares the output fields of this spout according to the provided
     * {@link backtype.storm.spout.Scheme}.
     *
     * Additionally declares an error stream (see {@link #ERROR_STREAM_NAME} for handling
     * malformed or empty messages to avoid infinite retry loops
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(serialisationScheme.getOutputFields());
        if (enableErrorStream) {
            declarer.declareStream(ERROR_STREAM_NAME, new Fields("deliveryTag", "bytes"));
        }
    }
}
