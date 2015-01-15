package edu.tuberlin.spex.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Date: 13.01.2015
 * Time: 13:26
 */
public class Datasets {

    Logger LOG = LoggerFactory.getLogger(Datasets.class);

    private String targetDirectory = "datasets";

    public enum GRAPHS {
        webBerkStan("http://snap.stanford.edu/data/web-BerkStan.txt.gz"),
        webNotreDame("http://snap.stanford.edu/data/web-NotreDame.txt.gz"),
        webStanford("http://snap.stanford.edu/data/web-Stanford.txt.gz"),
        youtTube("http://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz"),
        dblp("http://snap.stanford.edu/data/bigdata/communities/com-dblp.ungraph.txt.gz"),
        patents("http://snap.stanford.edu/data/cit-Patents.txt.gz"),
        liveJournal("http://snap.stanford.edu/data/bigdata/communities/com-lj.ungraph.txt.gz");

        private String url;

        GRAPHS(String url) {
            this.url = url;
        }
    }

    public Path get(GRAPHS dataset) throws IOException {

        String location = dataset.url;

        String name = com.google.common.io.Files.getNameWithoutExtension(location);
        String extension = com.google.common.io.Files.getFileExtension(location);

        String filename = Joiner.on(".").join(name, extension);

        Path path = Paths.get(targetDirectory, filename);
        if(Files.exists(path)) {
            LOG.info("Using cached copy for {}", location);
        } else {
            download(filename, location);
        }
        return path;
    }

    private void download(String name, String location) throws IOException {
        LOG.info("Downloading {}", location);
        URL website = new URL(location);

        Stopwatch stopwatch = new Stopwatch().start();
        ReadableByteChannel rbc = Channels.newChannel(website.openStream());

        File dir = new File(targetDirectory);
        if (!dir.isDirectory() && !dir.mkdirs()) {
            LOG.error("Can't create directory at {} ", targetDirectory);
        }
        FileOutputStream fos = new FileOutputStream(new File(targetDirectory, name));
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

        LOG.info("Downloaded in {}", stopwatch.stop().toString());
        fos.close();
        rbc.close();
    }

}
