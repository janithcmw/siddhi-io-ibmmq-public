/*
 *  Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.extension.siddhi.io.ibmmq.source;

import com.ibm.mq.jms.MQConnection;
import com.ibm.mq.jms.MQQueueConnectionFactory;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.source.SourceEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * IBM MQ message consumer class which retrieve messages from the queue.
 **/
public class IBMMessageConsumerThread implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(IBMMessageConsumerThread.class);
    private SourceEventListener sourceEventListener;
    private MQConnection connection;
    private MessageConsumer messageConsumer;
    private volatile boolean paused;
    private ReentrantLock lock;
    private Condition condition;
    private String queueName;
    private IBMMessageConsumerBean ibmMessageConsumerBean;
    private MQQueueConnectionFactory mqQueueConnectionFactory;
    private AtomicBoolean isInactive = new AtomicBoolean(true);
    private ConnectionRetryHandler connectionRetryHandler;

    public IBMMessageConsumerThread(SourceEventListener sourceEventListener,
                                    IBMMessageConsumerBean ibmMessageConsumerBean,
                                    MQQueueConnectionFactory mqQueueConnectionFactory,
                                    ConnectionRetryHandler connectionRetryHandler)
            throws JMSException, ConnectionUnavailableException {
        this.ibmMessageConsumerBean = ibmMessageConsumerBean;
        this.mqQueueConnectionFactory = mqQueueConnectionFactory;
        this.sourceEventListener = sourceEventListener;
        this.queueName = ibmMessageConsumerBean.getQueueName();
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
        this.connectionRetryHandler = connectionRetryHandler;

        connect();
    }

    @Override
    public void run() {
        while (!isInactive.get()) {
            try {
                if (paused) {
                    lock.lock();
                    try {
                        condition.await();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lock.unlock();
                    }
                }

                Message message = messageConsumer.receive();
                if (message instanceof MapMessage) {
                    Map<String, Object> event = new HashMap<>();
                    MapMessage mapEvent = (MapMessage) message;
                    Enumeration<String> mapNames = mapEvent.getMapNames();
                    while (mapNames.hasMoreElements()) {
                        String key = mapNames.nextElement();
                        event.put(key, mapEvent.getObject(key));
                    }
                    sourceEventListener.onEvent(event, null);
                } else if (message instanceof TextMessage) {
                    String event = ((TextMessage) message).getText();
                    sourceEventListener.onEvent(event, null);
                } else if (message instanceof ByteBuffer) {
                    sourceEventListener.onEvent(message, null);
                }
            } catch (Throwable t) {
                logger.error("Exception occurred during consuming messages: " + t.getMessage(), t);
                connectionRetryHandler.onError(t);
            }

        }
    }

    void pause() {
        paused = true;
    }

    void resume() {
        paused = false;
        try {
            lock.lock();
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void shutdownConsumer() {
        isInactive.set(true);
        try {
            if (Objects.nonNull(messageConsumer)) {
                messageConsumer.close();
            }
        } catch (JMSException e) {
            logger.error("Error occurred while closing the consumer for the queue: " + queueName + ". ", e);
        }

        try {
            if (Objects.nonNull(connection)) {
                connection.close();
            }
        } catch (JMSException e) {
            logger.error("Error occurred while closing the IBM MQ connection for the queue: " + queueName + ". ", e);
        }
    }

    public void connect() throws ConnectionUnavailableException {
        try {
            if (ibmMessageConsumerBean.isSecured()) {
                connection = (MQConnection) mqQueueConnectionFactory.createConnection(
                        ibmMessageConsumerBean.getUserName(), ibmMessageConsumerBean.getPassword());
            } else {
                connection = (MQConnection) mqQueueConnectionFactory.createConnection();
            }
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(ibmMessageConsumerBean.getDestinationName());
            this.messageConsumer = session.createConsumer(queue);
            this.connection.start();
            isInactive.set(false);
        } catch (JMSException e) {
            throw new ConnectionUnavailableException(e.getMessage(), e);
        }
    }

    public String getQueueName() {
        return queueName;
    }
}
