package org.corfudb.microbenchmarks;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.cmdlets.ICmdlet;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by amytai on 5/6/16.
 */
@Slf4j
public class AppendBenchmark {
    private static final String USAGE =
            "AppendBenchmark, append to streams based on backpointer or Replex.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tAppendBenchmark  -c <config> [-x <replexes>] [-l <numLay>] [-q <numSeq>] [-s <numStreams>] [-a <numAppends>] [-r] [-d <level>] [-m <numClients>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -c <config>, --config=<config>                 The config string to pass to the org.corfudb.runtime. \n"
                    + "                                                A comma-delimited list of Corfu servers. These will be read\n"
                    + "                                                in the order [-l], [-q], [...]. [...] denotes the leftover\n"
                    + "                                                Corfu servers, which are considered LUs.\n"
                    + " -x <replexes>, --replexes=<replexes>           A config string to pass to the org.corfudb.runtime, \n"
                    + "                                                denoting the locations of the Replex LU servers. \n"
                    + " -l <numLay>, --numLay=<numLay>                 Number of layout servers to use in the benchmark. [default: 1] \n"
                    + " -q <numSeq>, --numSeq=<numSeq>                 Number of sequencers to use in the benchmark. [default: 1] \n"
                    + " -s <numStreams>, --numStreams=<numStreams>     Number of streams to use in the benchmark. [default: 10] \n"
                    + " -a <numAppends>, --numAppends=<numAppends>     Number of appends to use in the benchmark. [default: 10000] \n"
                    + " -r                                             If used, flag denotes use Replex instead of backpointers. \n"
                    + " -d <level>, --log-level=<level>                Set the logging level, valid levels are: \n"
                    + "                                                ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -m <numClients>, --numClients=<numClients>     Number of clients to wait for before starting benchmark. [default: 1] \n"
                    + " -h, --help                                     Show this screen\n"
                    + " --version                                      Show version\n";

    public static void main(String[] args) throws Exception {
        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // TODO: Currently, -ns and -nl are ignored.
        // First get all the servers.
        String servers = (String) opts.get("--config");

        List<String> addressPortServers = Pattern.compile(",")
                .splitAsStream(servers)
                .map(String::trim)
                .collect(Collectors.toList());
        String layoutH = addressPortServers.get(0).split(":")[0];
        Integer layoutP = Integer.parseInt(addressPortServers.get(0).split(":")[1]);

        int LUstripes = (addressPortServers.size() - 2) / 2;
        //List<String> LUServers1 = addressPortServers.subList(2, 2 + LUServersPerReplica);
        //List<String> LUServers2 = addressPortServers.subList(2+LUServersPerReplica, addressPortServers.size());
        List<Layout.LayoutStripe> stripes = new ArrayList<>(LUstripes);
        int startIndex = 2;
        for (int i = 0; i < stripes.size(); i++) {
            stripes.add(new Layout.LayoutStripe(addressPortServers.subList(startIndex, startIndex + 2)));
            startIndex+=2;
        }

        // Create a client routers and set layout.
        log.trace("Creating layoutRouter for {}:{}", layoutH, layoutP);
        NettyClientRouter layoutRouter = new NettyClientRouter(layoutH, layoutP);
        layoutRouter.addClient(new BaseClient())
                .addClient(new LayoutClient())
                .start();

        Layout testLayout;
        if ((boolean) opts.get("-r")) {
            String replexServers = (String) opts.get("--replexes");

            List<String> addressPortReplexServers = Pattern.compile(",")
                    .splitAsStream(replexServers)
                    .map(String::trim)
                    .collect(Collectors.toList());
            Layout.LayoutSegment ls = new Layout.LayoutSegment(
                    Layout.ReplicationMode.REPLEX_REPLICATION,
                    0L,
                    -1L,
                    Collections.singletonList(
                            new Layout.LayoutStripe(addressPortServers.subList(2, addressPortServers.size()))));

            ls.setReplexStripes(Collections.singletonList(new Layout.LayoutStripe(addressPortReplexServers)));

            testLayout = new Layout(
                    Collections.singletonList(addressPortServers.get(0)),
                    Collections.singletonList(addressPortServers.get(1)),
                    Collections.singletonList(ls),
                    0L
            );
        } else {
            testLayout = new Layout(
                    Collections.singletonList(addressPortServers.get(0)),
                    Collections.singletonList(addressPortServers.get(1)),
                    Collections.singletonList(new Layout.LayoutSegment(
                            Layout.ReplicationMode.CHAIN_REPLICATION,
                            0L,
                            -1L,
                            stripes
                    )),
                    0L
            );
        }
        layoutRouter.getClient(LayoutClient.class).bootstrapLayout(testLayout).get();

        CorfuRuntime rt = new CorfuRuntime(addressPortServers.get(0)).connect();

        // Coordinate with other clients through the sequencer
        int numClients = Integer.parseInt((String) opts.get("--numClients")) - 1;
        rt.getSequencerView().nextToken(Collections.singleton(new UUID(0,0)), 1);
        while (rt.getSequencerView().nextToken(Collections.singleton(new UUID(0,0)), 0).getToken() != numClients) ;


        // Now we start the test.
        // TODO: FILL IN TEST BODY HERE.
        int numStreams = Integer.parseInt((String) opts.get("--numStreams"));
        List<UUID> streams = createStreams(numStreams);

        int numAppends = Integer.parseInt((String) opts.get("--numAppends"));
        long start;
        long end;

        Thread[] threads = new Thread[32];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new AppendBenchmarkThread(rt, numAppends, streams, (boolean) opts.get("-r")), "thread-" + i);
        }
        start = System.currentTimeMillis();
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        end = System.currentTimeMillis();

        System.out.println(ansi().fg(GREEN).a("SUCCESS").reset());
        System.out.printf("Time to completion: %d ms\n", end - start);
        double throughput = ((long) numAppends * 32 * 1000) / (end-start);
        System.out.printf("Throughput: %f ops / sec\n", throughput);
    }

    private static List<UUID> createStreams(int numStreams) {
        List<UUID> streams = new ArrayList<>();
        for (int i = 0; i < numStreams; i++) {
            streams.add(UUID.randomUUID());
        }
        return streams;
    }

    private static void configureBase(Map<String, Object> opts)
    {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        switch ((String)opts.get("--log-level"))
        {
            case "ERROR":
                root.setLevel(Level.ERROR);
                break;
            case "WARN":
                root.setLevel(Level.WARN);
                break;
            case "INFO":
                root.setLevel(Level.INFO);
                break;
            case "DEBUG":
                root.setLevel(Level.DEBUG);
                break;
            case "TRACE":
                root.setLevel(Level.TRACE);
                break;
            default:
                root.setLevel(Level.INFO);
                System.out.println("Level " + opts.get("--log-level") + " not recognized, defaulting to level INFO");
        }
        root.debug("Arguments are: {}", opts);
    }
}

class AppendBenchmarkThread implements Runnable {
    private CorfuRuntime rt;
    private int numAppends;
    private List<UUID> streams;
    private int numStreams;
    private boolean replex;

    private Random r = new Random(System.currentTimeMillis());
    private Object data = randomData(128);

    public AppendBenchmarkThread(CorfuRuntime rt, int numAppends, List<UUID> streams, boolean replex) {
        this.rt = rt;
        this.numAppends = numAppends;
        this.streams = streams;
        this.numStreams = streams.size();
        this.replex = replex;
    }

    public void run() {
        if (replex) {
            for (int i = 0; i < numAppends; i++) {
                rt.getReplexStreamsView().write(Collections.singleton(streams.get(r.nextInt(numStreams))), data);
            }
        } else {
            for (int i = 0; i < numAppends; i++) {
                rt.getStreamsView().write(Collections.singleton(streams.get(r.nextInt(numStreams))), data);
            }
        }
    }

    private Object randomData(int length) {
        SecureRandom r = new SecureRandom();
        byte data[] = new byte[length];
        r.nextBytes(data);
        return data;
    }
}
