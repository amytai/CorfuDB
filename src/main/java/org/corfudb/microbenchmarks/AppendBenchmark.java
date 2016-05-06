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
                    + "\tAppendBenchmark  -c <config> [-nl <numLay>] [-ns <numSeq>] [-s <numStreams>] [-n <numAppends>] [-r] [-d <level>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -c <config>, --config=<config>                 The config string to pass to the org.corfudb.runtime. \n"
                    + "                                                A comma-delimited list of Corfu servers. These will be read\n"
                    + "                                                in the order [-nl], [-ns], [...]. [...] denotes the leftover\n"
                    + "                                                Corfu servers, which are considered LUs.\n"
                    + " -nl <numLay>, --numLay=<numLay>                Number of layout servers to use in the benchmark. [default: 1] \n"
                    + " -ns <numSeq>, --numSeq=<numSeq>                Number of sequencers to use in the benchmark. [default: 1] \n"
                    + " -s <numStreams>, --numStreams=<numStreams>     Number of streams to use in the benchmark. [default: 10] \n"
                    + " -n <numAppends>, --numAppends=<numAppends>     Number of appends to use in the benchmark. [default: 10K] \n"
                    + " -r                                             If used, flag denotes use Replex instead of backpointers. \n"
                    + " -d <level>, --log-level=<level>                Set the logging level, valid levels are: \n"
                    + "                                                ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
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

        String sequencerH = addressPortServers.get(1).split(":")[0];
        Integer sequencerP = Integer.parseInt(addressPortServers.get(1).split(":")[1]);

        List<String> LUServers = addressPortServers.subList(2, addressPortServers.size());


        // Create a client routers and set layout.
        log.trace("Creating layoutRouter for {}:{}", layoutH, layoutP);
        NettyClientRouter layoutRouter = new NettyClientRouter(layoutH, layoutP);
        layoutRouter.addClient(new BaseClient())
                .addClient(new LayoutClient())
                .start();

        Layout testLayout = new Layout(
                Collections.singletonList(addressPortServers.get(0)),
                Collections.singletonList(addressPortServers.get(1)),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(
                                new Layout.LayoutStripe(
                                        LUServers
                                )
                        )
                )),
                0L
        );
        layoutRouter.getClient(LayoutClient.class).bootstrapLayout(testLayout).get();

       /* log.trace("Creating seqRouter for {}:{}", layoutH, layoutP);
        NettyClientRouter seqRouter = new NettyClientRouter(sequencerH, sequencerP);
        seqRouter.addClient(new BaseClient())
                .addClient(new SequencerClient())
                .start();*/

        // Get a org.corfudb.runtime instance from the options.
        CorfuRuntime rt = new CorfuRuntime(addressPortServers.get(0)).connect();

        // Now we start the test.
        Set<UUID> streams = createStreams(Integer.parseInt((String) opts.get("--numStreams")));

        rt.getStreamsView().write(streams, randomData(512));


        System.out.println(ansi().fg(GREEN).a("SUCCESS").reset());
    }

    private static Set<UUID> createStreams(int numStreams) {
        Set<UUID> streams = new HashSet<>();
        for (int i = 0; i < numStreams; i++) {
            streams.add(UUID.randomUUID());
        }
        return streams;
    }

    private static Object randomData(int length) {
        SecureRandom r = new SecureRandom();
        byte data[] = new byte[length];
        r.nextBytes(data);
        return data;
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
