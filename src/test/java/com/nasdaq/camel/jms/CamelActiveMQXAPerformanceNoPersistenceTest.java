package com.nasdaq.camel.jms;

import com.atomikos.icatch.jta.UserTransactionImp;
import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jms.AtomikosConnectionFactoryBean;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.XAConnectionFactory;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.UriEndpointComponent;
import org.apache.camel.spring.SpringRouteBuilder;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Scope;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.jta.JtaTransactionManager;

/**
 * Test code based on tests published in the conversation at:
 * http://camel.465427.n5.nabble.com/Camel-JMS-Performance-is-ridiculously-worse-than-pure-Spring-DMLC-td5716998.html
 *
 * @author Ola Theander <ola.theander@nasdaqomx.com>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CamelActiveMQXAPerformanceNoPersistenceTest extends CamelSpringTestSupport {

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
            broker.setPersistent(false);
            broker.setUseJmx(false);
            broker.addConnector("tcp://localhost:61616");
            broker.start();
            return broker;
        }

        @Bean(name = "activemq")
        public JmsComponent getActiveMQBean() {
            return ActiveMQComponent.activeMQComponent("tcp://localhost:61616");
        }

        @Bean(name = "amqxa")
        @DependsOn("brokerService")
        public UriEndpointComponent createProtocolEndpoint(BeanFactory beanFactory, JtaTransactionManager jtam) {
            final ActiveMQXAConnectionFactory amqConnectionFactory = new ActiveMQXAConnectionFactory();
            /**
             * jms.prefetchPolicy.all=0 to avoid the rollback:
             *
             * JmsConsumer[INTERNAL_MT_IN.QUEUE]] DEBUG o.a.a.ActiveMQMessageConsumer - on close, rollback duplicate:
             * ID:SE10WS01439-50920-1413812301395-1:18:2:1:1
             */
            // JmsConsumer[INTERNAL_MT_IN.QUEUE]] DEBUG o.a.a.ActiveMQMessageConsumer - on close,
            // rollback duplicate: ID:SE10WS01439-50920-1413812301395-1:18:2:1:1
            final String brokerURL = String.format("tcp://%s:%d?jms.prefetchPolicy.all=0",
                    "localhost",
                    61616);
            amqConnectionFactory.setBrokerURL(brokerURL);
            final AtomikosConnectionFactoryBean atomikosCF = createConnectionFactory(
                    beanFactory,
                    amqConnectionFactory,
                    8,
                    "amqxa");

            final JmsComponent jms = new JmsComponent();
            jms.setConnectionFactory(atomikosCF);
            jms.setTransactionManager(jtam);
            jms.setTransacted(true);
            jms.setReceiveTimeout(2000);
            jms.setCacheLevelName("CACHE_NONE");
            return jms;
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

        @Bean(name = "atomikosConnectionFactoryBean",
                initMethod = "init",
                destroyMethod = "close")
        @Scope(value = SCOPE_PROTOTYPE)
        public AtomikosConnectionFactoryBean getAtomikosConnectionFactory(XAConnectionFactory connectionFactory,
                int poolSize,
                String uniqueResourceName) {
            final AtomikosConnectionFactoryBean cf = new AtomikosConnectionFactoryBean();
            cf.setPoolSize(poolSize);
            cf.setXaConnectionFactory(connectionFactory);
            cf.setUniqueResourceName(uniqueResourceName);

            return cf;
        }

        private AtomikosConnectionFactoryBean createConnectionFactory(BeanFactory beanFactory,
                XAConnectionFactory connectionFactory,
                int poolSize,
                String uniqueResourceName) {
            final AtomikosConnectionFactoryBean atomikosCF
                    = (AtomikosConnectionFactoryBean) beanFactory.getBean("atomikosConnectionFactoryBean",
                            connectionFactory,
                            poolSize,
                            uniqueResourceName);

            return atomikosCF;
        }
    }

    @Autowired
    @Qualifier("amqxa")
    private UriEndpointComponent amqxaUri;

    @Autowired
    private JtaTransactionManager jtm;

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

    @Test
    public void testReadAndStoreXAAMQ() throws Exception {
        template.setDefaultEndpointUri("activemq:queue:test");

        System.out.println("Creating " + counter + " messages.");
        for (int i = 0; i < counter; i++) {
            template.sendBody(PAYLOAD);
        }
        System.out.println("Done creating messages.");

        final long millis = System.currentTimeMillis();
        final CountDownLatch latch = new CountDownLatch(counter);
        context.addRoutes(new SpringRouteBuilder() {
            public void configure() throws Exception {
                from("amqxa:queue:test")
                        .transacted()
                        .process(new Processor() {
                            public void process(Exchange exchange) throws
                            Exception {
                                latch.countDown();
//                                log.info(exchange.getIn().getBody(String.class));
                            }
                        })
                        .to("amqxa:queue:out");
            }
        });

        assertTrue(latch.await(5, TimeUnit.MINUTES));
        System.out.println("testReadAndStoreXAAMQ consumed " + counter + " messages in " + (System.currentTimeMillis() - millis) + " ms.");
    }

    @Override
    protected AbstractApplicationContext createApplicationContext() {
        return ac;
    }
}
