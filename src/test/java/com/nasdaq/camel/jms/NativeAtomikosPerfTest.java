package com.nasdaq.camel.jms;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jms.AtomikosConnectionFactoryBean;
import com.atomikos.jms.extra.MessageDrivenContainer;
import com.atomikos.jms.extra.SingleThreadedJmsSenderTemplate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.jms.XAConnectionFactory;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.jta.JtaTransactionManager;

/**
 * Test code based on tests published in the conversation at:
 * http://camel.465427.n5.nabble.com/Camel-JMS-Performance-is-ridiculously-worse-than-pure-Spring-DMLC-td5716998.html
 *
 * Optimizations for Atomikos as discussed in documentation and
 * http://www.atomikos.com/Documentation/OptimizingJmsPerformance
 *
 * @author Ola Theander <ola.theander@nasdaqomx.com>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class NativeAtomikosPerfTest extends CamelSpringTestSupport {

    private static final String PAYLOAD
            = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            + "xxxxxxxxxxxxxxxxxxxxxxxx";

    @Configuration
    static class Config {

        @Bean(name = "brokerService", destroyMethod = "stop")
        public BrokerService createBrokerService() throws Exception {
            final BrokerService broker = new BrokerService();
            broker.setPersistent(true);
            broker.setUseJmx(false);
            broker.addConnector("tcp://localhost:61616");
            broker.start();
            return broker;
        }

        @Bean(name = "atomikosTransactionManager",
                initMethod = "init",
                destroyMethod = "close")
        public UserTransactionManager getTransactionManager() {
            final UserTransactionManager userTransactionManager = new UserTransactionManager();
            userTransactionManager.setForceShutdown(false);
            return userTransactionManager;
        }

        @Bean(name = "atomikosUserTransaction")
        public UserTransaction getUserTransaction() throws SystemException {
            final UserTransactionImp userTransactionImp = new UserTransactionImp();
            userTransactionImp.setTransactionTimeout(2000);
            return userTransactionImp;
        }

        @Bean(name = "jtaTransactionManager")
        public JtaTransactionManager getJtaTransactionManager(UserTransactionManager transactionManager,
                @Qualifier("atomikosUserTransaction") UserTransaction userTransaction) {
            final JtaTransactionManager jta = new JtaTransactionManager();
            jta.setTransactionManager(transactionManager);
            jta.setUserTransaction(userTransaction);
            return jta;
        }

        @Bean
        public XAConnectionFactory getXAConnectionFactory() {
            final ActiveMQXAConnectionFactory amqConnectionFactory = new ActiveMQXAConnectionFactory();
            final String brokerURL = String.format("tcp://%s:%d?jms.prefetchPolicy.all=0",
                    "localhost",
                    61616);
            amqConnectionFactory.setBrokerURL(brokerURL);
            return amqConnectionFactory;
        }

        @Bean(name = "atomikosConnectionFactoryBean",
                initMethod = "init",
                destroyMethod = "close")
        public AtomikosConnectionFactoryBean getAtomikosConnectionFactory(XAConnectionFactory connectionFactory) {
            final AtomikosConnectionFactoryBean cf = new AtomikosConnectionFactoryBean();
            cf.setPoolSize(8);
            cf.setXaConnectionFactory(connectionFactory);
            cf.setUniqueResourceName("amqxa");
            return cf;
        }
    }

    @Autowired
    @Qualifier("atomikosConnectionFactoryBean")
    private AtomikosConnectionFactoryBean connectionFactory;

    @Autowired
    private AbstractApplicationContext ac;

    private final int counter = 3000;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * This test uses the optimized MessageDrivenContainer and JmsSenderTemplate from
     * http://www.atomikos.com/Documentation/OptimizingJmsPerformance
     */
    @Test
    public void testOptimizedAtomikosReceiverSender() throws JMSException, InterruptedException {
        template.setDefaultEndpointUri("activemq:queue:test");

        log.info("Creating " + counter + " messages.");
        for (int i = 0; i < counter; i++) {
            template.sendBody(PAYLOAD);
        }
        log.info("Done creating messages.");

        //create a queues for ActiveMQ
        final ActiveMQQueue sourceQueue = new ActiveMQQueue();
        sourceQueue.setPhysicalName("test");
        final ActiveMQQueue destQueue = new ActiveMQQueue();
        destQueue.setPhysicalName("out");

        //setup the Atomikos session for sending messages on
        final SingleThreadedJmsSenderTemplate senderSession = new SingleThreadedJmsSenderTemplate();
        senderSession.setAtomikosConnectionFactoryBean(connectionFactory);
        senderSession.setDestination(destQueue);
        senderSession.init();

        final CountDownLatch latch = new CountDownLatch(counter);

        final MessageDrivenContainer container = new MessageDrivenContainer();
        container.setPoolSize(1);
        container.setTransactionTimeout(120);
        container.setNotifyListenerOnClose(true);
        container.setAtomikosConnectionFactoryBean(connectionFactory);
        container.setDestination(sourceQueue);
        container.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                // here we are if a message is received; a transaction
                // as been started before this method has been called.
                // this is done for us by the MessageDrivenContainer...

                if (message instanceof TextMessage) {
                    final TextMessage tmsg = (TextMessage) message;
                    try {
//                        System.out.println("Transactional receive of message: "
//                                + tmsg.getText());
                        //send a message
                        senderSession.sendObjectMessage(tmsg.toString());

                        latch.countDown();
                    } catch (JMSException ex) {
                        ex.printStackTrace();
                        // throw runtime exception tof orce rollback of
                        // transaction
                        throw new RuntimeException("Rollback due to error");
                    }
                }
            }
        });

        final long millis = System.currentTimeMillis();
        container.start();
        assertTrue(latch.await(5, TimeUnit.MINUTES));
        log.info("testOptimizedAtomikosReceiverSender consumed " + counter + " messages in " + (System.currentTimeMillis() - millis) + " ms.");

        // when finished: close the sender session
        senderSession.close();
    }

    @Override
    protected AbstractApplicationContext createApplicationContext() {
        return ac;
    }
}
