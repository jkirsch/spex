package edu.tuberlin.spex.experiments;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.io.CountingInputStream;
import edu.tuberlin.spex.utils.CompressionHelper;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * 21.04.2015.
 */
public class ExperimentDatasets {

    private static final Logger LOG = LoggerFactory.getLogger(ExperimentDatasets.class);
    private static final String targetDirectory = "datasets";

    public static Path get(Matrix dataset) throws IOException, ArchiveException {

        String location = dataset.url;

        String name = com.google.common.io.Files.getNameWithoutExtension(location);
        String extension = com.google.common.io.Files.getFileExtension(location);

        if ("gz".equals(extension)) {
            name = com.google.common.io.Files.getNameWithoutExtension(name);
        }

        String filename = Joiner.on(".").join(name, "mtx");

        Path path = Paths.get(targetDirectory, filename);
        if (Files.exists(path)) {
            LOG.debug("Using cached copy for {}", location);
        } else {
            download(location);
        }
        return path;
    }

    private static void download(String location) throws IOException, ArchiveException {
        LOG.info("Downloading {}", location);
        URL website = new URL(location);

        Stopwatch stopwatch = Stopwatch.createStarted();


        File dir = new File(targetDirectory);
        if (!dir.isDirectory() && !dir.mkdirs()) {
            LOG.error("Can't create directory at {} ", targetDirectory);
        }

        CountingInputStream countingInputStream = new CountingInputStream(website.openStream());

        // decompress if needed using GZIP
        InputStream input = CompressionHelper.getDecompressionStream(countingInputStream);

        TarArchiveInputStream archiveInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", input);
        TarArchiveEntry entry;
        while ((entry = (TarArchiveEntry) archiveInputStream.getNextEntry()) != null) {

            String filename = FilenameUtils.getName(entry.getName());
            if (FilenameUtils.getExtension(filename).equals("mtx")) {
                File outputFile = new File(targetDirectory, filename);
                FileOutputStream fos = new FileOutputStream(outputFile);
                fos.getChannel().transferFrom(Channels.newChannel(archiveInputStream), 0, Long.MAX_VALUE);
                fos.close();
                // assume only one file per archive
                break;
            }
        }


        stopwatch.stop();

        long elapsedSeconds = stopwatch.elapsed(TimeUnit.SECONDS);

        LOG.info("Downloaded in {} with {} in {} Kb/s",
                stopwatch.toString(),
                FileUtils.byteCountToDisplaySize(countingInputStream.getCount()),
                elapsedSeconds > 0 ? (countingInputStream.getCount() / 1024 / elapsedSeconds)
                        : (countingInputStream.getCount() / 1024 / stopwatch.elapsed(TimeUnit.MILLISECONDS)) * 1000);

        archiveInputStream.close();
    }

    public enum Matrix {

        mhd1280b("http://www.cise.ufl.edu/research/sparse/MM/Bai/mhd1280b.tar.gz"),
        conf5_0("http://www.cise.ufl.edu/research/sparse/MM/QCD/conf5_0-4x4-10.tar.gz"),
        rlfprim("http://www.cise.ufl.edu/research/sparse/MM/Meszaros/rlfprim.tar.gz"),
        lpi_box1("http://www.cise.ufl.edu/research/sparse/MM/LPnetlib/lpi_box1.tar.gz"),
        sgpf5y6("http://www.cise.ufl.edu/research/sparse/MM/Mittelmann/sgpf5y6.tar.gz");

        private String url;

        Matrix(String url) {
            this.url = url;
        }

    }

}
