package com.rapportive.storm.spout;

import backtype.storm.spout.Scheme;
import com.rapportive.storm.amqp.QueueDeclaration;

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
}
