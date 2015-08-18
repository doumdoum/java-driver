package com.datastax.driver.core;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.TestUtils.nonDebouncingQueryOptions;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.scassandra.http.client.ConnectionsClient.CloseType.CLOSE;

public class HostConnectionPoolMultiTest {

    private SCassandraCluster scassandra;

    private Cluster cluster;

    private Session session;

    @BeforeMethod(groups={"short", "long"})
    private void setUp() {
        scassandra = new SCassandraCluster(2);
    }

    @AfterMethod(groups={"short", "long"})
    private void tearDown() {
        if(cluster != null) {
            cluster.close();
        }
        scassandra.stop();
    }

    private void createCluster(int core, int max) {
        PoolingOptions poolingOptions = new PoolingOptions().setConnectionsPerHost(LOCAL, core, max);
        SocketOptions socketOptions = new SocketOptions().setReadTimeoutMillis(1000);
        cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withQueryOptions(nonDebouncingQueryOptions())
                .withPoolingOptions(poolingOptions)
                .withSocketOptions(socketOptions)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                .build();
        cluster.connect();
    }

    @Test(groups="short")
    public void should_mark_host_down_if_all_connections_fail_on_init() {
        // Prevent any connections on node 2.
        scassandra.instance(2).serverClient().disableListener();
        createCluster(8,8);

        // Node 2 should be in a down state while node 1 stays up.
        assertThat(cluster).host(2).goesDownWithin(10, SECONDS);
        assertThat(cluster).host(1).isUp();

        // Node 2 should come up as soon as it is able to reconnect.
        scassandra.instance(2).serverClient().enableListener();
        assertThat(cluster).host(2).comesUpWithin(2, SECONDS);
    }

    @Test(groups="short")
    public void should_replace_control_connection_if_it_goes_down_but_host_remains_up() {
        createCluster(1,2);

        // Ensure control connection is on node 1.
        assertThat(cluster).usesControlHost(1);

        // Identify the socket associated with the control connection.
        Connection controlConnection = cluster.manager.controlConnection.connectionRef.get();
        InetSocketAddress controlSocket = (InetSocketAddress)controlConnection.channel.localAddress();

        // Close the control connection.
        scassandra.instance(1).connectionsClient()
                .closeConnection(CLOSE, controlSocket);

        // Sleep reconnect interval * 2 to allow time to reconnect.
        Uninterruptibles.sleepUninterruptibly(2, SECONDS);

        // Ensure the control connection was replaced and host 1 remains up.
        assertThat(cluster).hasOpenControlConnection()
                .host(1).isUp();
        assertThat(cluster.manager.controlConnection.connectionRef.get()).isNotEqualTo(controlConnection);
    }
}
