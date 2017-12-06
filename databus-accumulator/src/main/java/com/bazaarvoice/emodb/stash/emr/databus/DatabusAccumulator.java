package com.bazaarvoice.emodb.stash.emr.databus;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;

public class DatabusAccumulator {

    public static void main(String args[]) {
        ArgumentParser argParser = ArgumentParsers.newFor("DatabusAccumulator").addHelp(true).build();

        argParser.addArgument("--cluster")
                .required(true)
                .nargs("1")
                .help("EmoDB cluster name");
        argParser.addArgument("--subscription")
                .required(true)
                .nargs("1")
                .help("Databus subscription name");
        argParser.addArgument("--apikey")
                .required(true)
                .nargs("1")
                .help("Databus API key name");

    }
}
