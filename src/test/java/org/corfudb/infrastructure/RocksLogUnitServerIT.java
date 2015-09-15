package org.corfudb.infrastructure;

import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.ExtntWrap;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.protocols.logunits.CorfuDBSimpleLogUnitProtocol;
import org.corfudb.runtime.view.ConfigurationMaster;
import org.corfudb.runtime.view.IConfigurationMaster;
import org.corfudb.util.CorfuInfrastructureBuilder;
import org.corfudb.util.Utils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

/**
 * Test the rebuild functionality in the Rocks LU.
 *
 * Created by amytai on 7/29/15.
 */
public class RocksLogUnitServerIT {

    CorfuDBRuntime cdr;
    CorfuDBSimpleLogUnitProtocol lu;
    private static RocksLogUnitServer ru = new RocksLogUnitServer();
    private static UUID uuid = UUID.randomUUID();

    static Map<String, Object> luConfigMap = new HashMap<String,Object>() {
        {
            put("capacity", 200000);
            put("ramdisk", true);
            put("pagesize", 4096);
            put("trim", 0);
        }
    };

    CorfuInfrastructureBuilder infrastructure =
            CorfuInfrastructureBuilder.getBuilder()
                    .addSequencer(9001, StreamingSequencerServer.class, "cdbsts", null)
                    .addLoggingUnit(9000, 0, RocksLogUnitServer.class, "cdbslu", luConfigMap)
                    .start(9002);


    private static String TESTFILE = "rocksTestFile";
    private static int PAGESIZE = 4096;
    private static int NUMPAGES = 100;

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    @Before
    public void getCDR() {
        cdr = CorfuDBRuntime.createRuntime(infrastructure.getConfigString());
        cdr.waitForViewReady();
        IConfigurationMaster cm = new ConfigurationMaster(cdr);
        cm.resetAll();
        lu = ((CorfuDBSimpleLogUnitProtocol) cdr.getView().getSegments().get(0).getGroups().get(0).get(0));
    }

    private static byte[] getTestPayload(int size)
    {
        byte[] test = new byte[size];
        for (int i = 0; i < size; i++)
        {
            test[i] = (byte)(i % 255);
        }
        return test;
    }

  //  @Test
    public void testRebuild() throws Exception {
        byte[] t = getTestPayload(1024);

        for (int i = 0; i < NUMPAGES; i++) {
            lu.write(i, Collections.singleton(uuid), t);
        }

        HashMap<String, Object> configMap = new HashMap<String, Object>();
        configMap.put("ramdisk", false);
        configMap.put("capacity", 1000);
        configMap.put("port", 0);
        configMap.put("pagesize", PAGESIZE);
        configMap.put("trim", -1);
        configMap.put("drive", TESTFILE);
        configMap.put("rebuild", "cdbslu://localhost:12803");
        Thread thread = new Thread(ru.getInstance(configMap));
        thread.start();

        // Wait for server thread to finish rebuilding
        while (!ru.isReady()) ;

        for (int i = 0; i < NUMPAGES; i++) {
            ExtntWrap ew = ru.get(0, uuid);
            assertNotNull(ew);
            assertThat(ew.getErr()).isEqualTo(ErrorCode.OK);
            assert(ew.isSetCtnt());
            byte[] o = ru.get(0, uuid).getCtnt().get(0).array();
            assertThat(t).isEqualTo(o);
        }
    }
}
