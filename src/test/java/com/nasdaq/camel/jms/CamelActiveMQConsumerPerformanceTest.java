package com.nasdaq.camel.jms;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.jms.JmsConfiguration;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.spring.SpringRouteBuilder;
import org.apache.camel.spring.spi.SpringTransactionPolicy;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test code based on tests published in the conversation at:
 * http://camel.465427.n5.nabble.com/Camel-JMS-Performance-is-ridiculously-worse-than-pure-Spring-DMLC-td5716998.html
 *
 * @author Ola Theander <ola.theander@nasdaqomx.com>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
//@SpringApplicationConfiguration(classes = Application.class)
public class CamelActiveMQConsumerPerformanceTest extends CamelTestSupport {

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

        @Bean(name = "activemq")
        public JmsComponent getActiveMQBean(JmsConfiguration jmsConfig) {
//            final ActiveMQComponent amq = ActiveMQComponent.activeMQComponent("tcp://localhost:61616");
            final ActiveMQComponent amq = ActiveMQComponent.activeMQComponent();
            amq.setConfiguration(jmsConfig);
            return amq;
        }

        @Bean
        public JmsConfiguration getJmsConfiguration(PooledConnectionFactory cf, JmsTransactionManager jtm) {
            final JmsConfiguration jmsConfiguration = new JmsConfiguration();
            jmsConfiguration.setConnectionFactory(cf);
            jmsConfiguration.setTransacted(true);
            jmsConfiguration.setTransactionManager(jtm);
            jmsConfiguration.setCacheLevelName("CACHE_NONE");
            return jmsConfiguration;
        }

        @Bean
        public JmsTransactionManager getJmsTransactionManager(PooledConnectionFactory cf) {
            final JmsTransactionManager jta = new JmsTransactionManager();
            jta.setConnectionFactory(cf);
            return jta;
        }

        @Bean
        public PooledConnectionFactory getConnectionFactory(ActiveMQConnectionFactory cf) {
            final PooledConnectionFactory pcf = new PooledConnectionFactory();
            pcf.setConnectionFactory(cf);
            return pcf;
        }

        @Bean
        public ActiveMQConnectionFactory getActiveMQConnectionFactory() {
            final ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory();
            cf.setBrokerURL("tcp://localhost:61616?jms.prefetchPolicy.all=1");
            cf.setWatchTopicAdvisories(false);
            return cf;
        }

        @Bean(name = "PROPAGATION_REQUIRED")
        public SpringTransactionPolicy getPropagationRequiredPolicy(JmsTransactionManager jmsTransactionManager) {
            return new SpringTransactionPolicy(
                    jmsTransactionManager);
        }

        @Bean(name = "PROPAGATION_REQUIRES_NEW")
        public SpringTransactionPolicy getPropagationRequiresNewPolicy(JmsTransactionManager jmsTransactionManager) {
            final SpringTransactionPolicy policy = new SpringTransactionPolicy(
                    jmsTransactionManager);
            policy.setPropagationBehaviorName("PROPAGATION_REQUIRES_NEW");
            return policy;
        }

    }

    @Autowired
    @Qualifier(value = "PROPAGATION_REQUIRES_NEW")
    private SpringTransactionPolicy policyReqNew;

    @Autowired
    @Qualifier(value = "PROPAGATION_REQUIRED")
    private SpringTransactionPolicy policyReq;
    
    private int counter = 3000;
    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(true);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:61616");
        broker.start();

        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        broker.stop();
    }

    @Test
    public void testOnlyReadAMQ() throws Exception {
        template.setDefaultEndpointUri("activemq:queue:test");

        log.info("Creating " + counter + " messages.");
        for (int i = 0; i < counter; i++) {
            template.sendBody(PAYLOAD);
        }
        log.info("Done creating messages.");

        final long millis = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(counter);
        context.addRoutes(new SpringRouteBuilder() {
            public void configure() throws Exception {

                from("activemq:queue:test?acknowledgementModeName=DUPS_OK_ACKNOWLEDGE")
                        .process(new Processor() {
                            public void process(Exchange exchange) throws
                            Exception {
                                latch.countDown();
//                                log.info(exchange.getIn().getBody(String.class));
                            }
                        });
            }
        });

        assertTrue(latch.await(1, TimeUnit.MINUTES));
        log.info("testOnlyReadAMQ consumed " + counter + " messages in " + (System.currentTimeMillis() - millis) + " ms.");
    }

    @Test
    public void testConcurrentReadAMQ() throws Exception {
        template.setDefaultEndpointUri("activemq:queue:test");

        log.info("Creating " + counter + " messages.");
        for (int i = 0; i < counter; i++) {
            template.sendBody(PAYLOAD);
        }
        log.info("Done creating messages.");

        final long millis = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(counter);
        context.addRoutes(new SpringRouteBuilder() {
            public void configure() throws Exception {

                from("activemq:queue:test?concurrentConsumers=8")
                        .process(new Processor() {
                            public void process(Exchange exchange) throws
                            Exception {
                                latch.countDown();
//                                log.info(exchange.getIn().getBody(String.class));
                            }
                        });
            }
        });

        assertTrue(latch.await(1, TimeUnit.MINUTES));
        log.info("testConcurrentReadAMQ consumed " + counter + " messages in " + (System.currentTimeMillis() - millis) + " ms.");
    }

    @Test
    public void testReadAndStoreAMQ() throws Exception {
        template.setDefaultEndpointUri("activemq:queue:test");

        log.info("Creating " + counter + " messages.");
        for (int i = 0; i < counter; i++) {
            template.sendBody(PAYLOAD);
        }
        log.info("Done creating messages.");

        final long millis = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(counter);
        context.addRoutes(new SpringRouteBuilder() {
            public void configure() throws Exception {

                from("activemq:queue:test?acknowledgementModeName=DUPS_OK_ACKNOWLEDGE")
                        .process(new Processor() {
                            public void process(Exchange exchange) throws
                            Exception {
                                latch.countDown();
//                                log.info(exchange.getIn().getBody(String.class));
                            }
                        })
                        .to("activemq:queue:out");
            }
        });

        assertTrue(latch.await(1, TimeUnit.MINUTES));
        log.info("testReadAndStoreAMQ consumed " + counter + " messages in " + (System.currentTimeMillis() - millis) + " ms.");
    }

    @Test
    public void testReadAndStoreTransactionalAMQ() throws Exception {
        template.setDefaultEndpointUri("activemq:queue:test");

        log.info("Creating " + counter + " messages.");
        for (int i = 0; i < counter; i++) {
            template.sendBody(PAYLOAD);
        }
        log.info("Done creating messages.");

        final long millis = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(counter);
        context.addRoutes(new SpringRouteBuilder() {
            public void configure() throws Exception {
                from("activemq:queue:test")
                        .transacted("PROPAGATION_REQUIRES_NEW")
                        .process(new Processor() {
                            public void process(Exchange exchange) throws
                            Exception {
                                latch.countDown();
//                                log.info(exchange.getIn().getBody(String.class));
                            }
                        })
                        .to("activemq:queue:out");
            }
        });

        assertTrue(latch.await(1, TimeUnit.MINUTES));
        log.info("testReadAndStoreTransactionalAMQ consumed " + counter + " messages in " + (System.currentTimeMillis() - millis) + " ms.");
    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry registry = super.createRegistry();
//        registry.bind("activemq",
//                ActiveMQComponent.activeMQComponent("tcp://localhost:61616"));
        registry.bind("PROPAGATION_REQUIRES_NEW", policyReqNew);
        registry.bind("PROPAGATION_REQUIRED", policyReq);
        return registry;
    }
}
