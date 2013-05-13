package main;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


import com.thetransactioncompany.jsonrpc2.JSONRPC2Request;

public class CMHMessageTest {

    // tests on topology listed at
    // http://www.cs.colostate.edu/~cs551/CourseNotes/Deadlock/DDCMHAlg.html
    static CMHHandler processes;

    static ServerAddress s1;
    static TransactionId t1;
    static CMHProcessor p1;

    static ServerAddress s2;
    static TransactionId t2;
    static CMHProcessor p2;

    static ServerAddress s3;
    static TransactionId t3;
    static CMHProcessor p3;

    static ServerAddress s4;
    static TransactionId t4;
    static CMHProcessor p4;

    static ServerAddress s5;
    static TransactionId t5;
    static CMHProcessor p5;
    
    static RPC rpc;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        processes = new CMHHandler();
        s1 = new ServerAddress(1, "server 1", 2000);
        t1 = new TransactionId(s1, 1);
        p1 = new CMHProcessor(t1);
        processes.add(1, p1);

        s2 = new ServerAddress(2, "server 2", 2000);
        t2 = new TransactionId(s2, 2);
        p2 = new CMHProcessor(t2);
        processes.add(2, p2);

        s3 = new ServerAddress(3, "server 3", 2000);
        t3 = new TransactionId(s3, 3);
        p3 = new CMHProcessor(t3);
        processes.add(3, p3);

        s4 = new ServerAddress(4, "server 4", 2000);
        t4 = new TransactionId(s4, 4);
        p4 = new CMHProcessor(t4);
        processes.add(4, p4);

        s5 = new ServerAddress(5, "server 5", 2000);
        t5 = new TransactionId(s5, 4);
        p5 = new CMHProcessor(t5);
        processes.add(5, p5);
        
        rpc = new RPC(s1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        processes.resetAllWaitingForTid();
    }

    @Test
    public void TestJSONMessage() {
        // make sure messages look correct
        CMHMessage m1 = new CMHMessage(1, 1, 2);
        assertEquals(
                m1.getJSONMessage(),
                "{\"method\":\"processMessage\",\"params\":{\"to\":2,\"initiator\":1,\"from\":1},\"jsonrpc\":\"2.0\"}");
    }

    @Test
    public void DeadlockingTopology() {
        processes.get(1).addWaitingForTid(3);
        processes.get(3).addWaitingForTid(5);
        processes.get(3).addWaitingForTid(2);
        processes.get(2).addWaitingForTid(1);
        processes.get(2).addWaitingForTid(4);

        // send message from p1 in current topology
        CMHMessage m1 = new CMHMessage(1, 1, 3);

        // start a message from 1 to 3
        //processes.get(3).processMessage(m1.getJSONMessage());
        
        RPC.send(s3, "processMessage", "001", m1.getArgs());
        JSONRPC2Request item = rpc.receive();
        System.out.println(item.toJSONString());

        assertEquals(true, processes.get(1).deadlocked);

        // number 2 shouldn't know that it's deadlocked from this iteration
        assertEquals(false, processes.get(2).deadlocked);
    }

    @Test
    public void NonDeadlockingTopology() {
        // the P3->P2 dependency is missing
        processes.get(1).addWaitingForTid(3);
        processes.get(3).addWaitingForTid(5);
        processes.get(2).addWaitingForTid(1);
        processes.get(2).addWaitingForTid(4);

        // send message from p1 in current topology
        CMHMessage m1 = new CMHMessage(1, 1, 3);

        // start a message from 1 to 3
        processes.get(3).processMessage(m1.getJSONMessage());

        assertEquals(false, processes.get(1).deadlocked);
    }
}
