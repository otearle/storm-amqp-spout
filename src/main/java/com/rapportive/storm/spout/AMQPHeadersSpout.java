package com.rapportive.storm.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rapportive.storm.amqp.QueueDeclaration;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by alt on 6/2/14.
 */
public class AMQPHeadersSpout extends BaseAMQPSpout{

    /**
     * Create a new AMQP spout.  When
     * {@link #open(java.util.Map, TopologyContext, SpoutOutputCollector)} is called, it
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
    public AMQPHeadersSpout(String host, int port, String username, String password, String vhost, QueueDeclaration queueDeclaration, Scheme scheme) {
        super(host, port, username, password, vhost, queueDeclaration, scheme);
    }

    /**
     * Create a new AMQP spout.  When
     * {@link #open(java.util.Map, TopologyContext, SpoutOutputCollector)} is called, it
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
    public AMQPHeadersSpout(String host, int port, String username, String password, String vhost, QueueDeclaration queueDeclaration, Scheme scheme, boolean requeueOnFail, boolean enableErrorStream, boolean autoAck) {
        super(host, port, username, password, vhost, queueDeclaration, scheme, requeueOnFail, enableErrorStream, autoAck);
    }

    @Override
    public void nextTuple() {
        if (this.spoutActive && this.amqpConsumer != null) {
            try {
                final QueueingConsumer.Delivery delivery = this.amqpConsumer.nextDelivery(WAIT_FOR_NEXT_MESSAGE);
                if (delivery == null) return;
                final Map<String, Object> headers = delivery.getProperties().getHeaders();
                final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                final byte[] message = delivery.getBody();
                String text_message = null;
                if (message != null) {
                    try {
                        text_message = new String(delivery.getBody(), "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    this.collector.emit(new Values(text_message, headers, deliveryTag));
                } else {
                    this.handleMalformedDelivery(deliveryTag, message);
                }
            } catch (ShutdownSignalException e) {
                log.warn("AMQP connection dropped, will attempt to reconnect...");
                Utils.sleep(WAIT_AFTER_SHUTDOWN_SIGNAL);
                reconnect();
            } catch (ConsumerCancelledException e) {
                log.warn("AMQP consumer cancelled, will attempt to reconnect...");
                Utils.sleep(WAIT_AFTER_SHUTDOWN_SIGNAL);
                reconnect();
            } catch (InterruptedException e) {
                // interrupted while waiting for message, big deal
            }
        }
    }
}
