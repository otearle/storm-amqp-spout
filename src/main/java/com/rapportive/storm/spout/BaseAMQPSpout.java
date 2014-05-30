package com.rapportive.storm.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by alt on 5/20/14.
 */
public class BaseAMQPSpout implements IRichSpout {
    protected static final Logger log = Logger.getLogger(BaseAMQPSpout.class);

    private final String amqpHost;
    private final int amqpPort;
    private final String amqpUsername;
    private final String amqpPassword;
    private final String amqpVhost;
    protected final Scheme serialisationScheme;

    protected transient boolean spoutActive = true;
    protected transient Connection amqpConnection;
    protected transient Channel amqpChannel;
    protected transient DefaultConsumer amqpConsumer;
    protected transient String amqpConsumerTag;
    protected SpoutOutputCollector collector;
    private int prefetchCount;

    /*
    #############################################
    CONSTRUCTORS
    #############################################
     */

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
     */
    public BaseAMQPSpout(String host, int port, String username, String password,
                     String vhost, QueueDeclaration queueDeclaration, Scheme scheme ) {
        this.amqpHost = host;
        this.amqpPort = port;
        this.amqpUsername = username;
        this.amqpPassword = password;
        this.amqpVhost = vhost;
        this.serialisationScheme = scheme;
    }

    public BaseAMQPSpout(Map spoutConfig) {
        this.amqpHost = (String) spoutConfig.get("rabbitmq_host");
        this.amqpPort = (int) spoutConfig.get("rabbitmq_port");
        this.amqpUsername = (String) spoutConfig.get("rabbitmq_username");
        this.amqpPassword = (String) spoutConfig.get("rabbitmq_password");
        this.amqpVhost = (String) spoutConfig.get("rabbitmq_vhost");
    }

    @Override
    void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector);

    @Override
    void close();

    @Override
    void activate();

    @Override
    void deactivate();

    @Override
    void nextTuple();

    @Override
    void ack(Object o);

    @Override
    void fail(Object o);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
