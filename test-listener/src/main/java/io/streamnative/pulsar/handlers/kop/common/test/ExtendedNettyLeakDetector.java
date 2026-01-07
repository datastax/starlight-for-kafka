/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.common.test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Netty {@link ResourceLeakDetector} that dumps detected leaks to files in a directory configured with
 * {@code NETTY_LEAK_DUMP_DIR}.
 *
 * <p>This is intended for CI usage to make leak reporting reliable even when test output is redirected to files.
 */
public class ExtendedNettyLeakDetector<T> extends ResourceLeakDetector<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtendedNettyLeakDetector.class);

    public static final String NETTY_CUSTOM_LEAK_DETECTOR_SYSTEM_PROPERTY_NAME =
            "io.netty.customResourceLeakDetector";
    public static final String USE_SHUTDOWN_HOOK_SYSTEM_PROPERTY_NAME =
            ExtendedNettyLeakDetector.class.getName() + ".useShutdownHook";
    public static final String EXIT_JVM_ON_LEAK_SYSTEM_PROPERTY_NAME =
            ExtendedNettyLeakDetector.class.getName() + ".exitJvmOnLeak";
    public static final String EXIT_JVM_DELAY_MILLIS_SYSTEM_PROPERTY_NAME =
            ExtendedNettyLeakDetector.class.getName() + ".exitJvmDelayMillis";
    public static final String SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS_SYSTEM_PROPERTY_NAME =
            ExtendedNettyLeakDetector.class.getName() + ".sleepAfterGCAndFinalizationMillis";
    public static final String NETTY_LEAK_DETECTION_ENV = "NETTY_LEAK_DETECTION";
    public static final String NETTY_LEAK_DUMP_DIR_ENV = "NETTY_LEAK_DUMP_DIR";

    private static final String MODE_REPORT = "report";
    private static final String MODE_FAIL_ON_LEAK = "fail_on_leak";
    private static final String MODE_OFF = "off";
    private static final long DEFAULT_EXIT_JVM_DELAY_MILLIS = 1000L;
    private static final long DEFAULT_SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS = 10L;

    private static final AtomicLong DUMP_COUNTER = new AtomicLong();
    private static final DateTimeFormatter DUMP_TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS", Locale.ROOT);

    private static final String MODE = normalizeMode(System.getenv(NETTY_LEAK_DETECTION_ENV));
    private static final File DUMP_DIR = new File(System.getenv().getOrDefault(NETTY_LEAK_DUMP_DIR_ENV,
            System.getProperty("java.io.tmpdir")));
    private static final boolean USE_SHUTDOWN_HOOK = Boolean.parseBoolean(
            System.getProperty(USE_SHUTDOWN_HOOK_SYSTEM_PROPERTY_NAME, "false"));
    private static boolean exitJvmOnLeak = Boolean.parseBoolean(
            System.getProperty(EXIT_JVM_ON_LEAK_SYSTEM_PROPERTY_NAME, "false"));
    private static final boolean DEFAULT_EXIT_JVM_ON_LEAK = exitJvmOnLeak;
    private static final long EXIT_JVM_DELAY_MILLIS = Long.parseLong(System.getProperty(
            EXIT_JVM_DELAY_MILLIS_SYSTEM_PROPERTY_NAME, String.valueOf(DEFAULT_EXIT_JVM_DELAY_MILLIS)));
    private static final long SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS = Long.parseLong(System.getProperty(
            SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS_SYSTEM_PROPERTY_NAME,
            String.valueOf(DEFAULT_SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS)));

    private static volatile String initialHint;

    static {
        if (MODE_OFF.equals(MODE)) {
            ResourceLeakDetector.setEnabled(false);
            ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        } else {
            maybeRegisterShutdownHook();
        }
    }

    public static void setInitialHint(String initialHint) {
        ExtendedNettyLeakDetector.initialHint = initialHint;
    }

    public static boolean isExtendedNettyLeakDetectorEnabled() {
        return ExtendedNettyLeakDetector.class.getName()
                .equals(System.getProperty(NETTY_CUSTOM_LEAK_DETECTOR_SYSTEM_PROPERTY_NAME));
    }

    @SuppressFBWarnings(value = "DM_GC",
            justification = "Used to increase reliability of Netty leak detection flushing in CI/test runs.")
    public static void triggerLeakDetection() {
        if (MODE_OFF.equals(MODE) || !isExtendedNettyLeakDetectorEnabled() || !isEnabled()) {
            return;
        }
        try {
            System.gc();
            System.runFinalization();
            Thread.sleep(SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        triggerLeakReporting();
    }

    private static void triggerLeakReporting() {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer();
        ByteBuf retainedSlice = buffer.retainedSlice();
        retainedSlice.release();
        buffer.release();
    }

    public static void disableExitJVMOnLeak() {
        exitJvmOnLeak = false;
    }

    public static void restoreExitJVMOnLeak() {
        triggerLeakDetection();
        exitJvmOnLeak = DEFAULT_EXIT_JVM_ON_LEAK;
    }

    public ExtendedNettyLeakDetector(Class<?> resourceType) {
        super(resourceType);
    }

    public ExtendedNettyLeakDetector(String resourceType) {
        super(resourceType);
    }

    public ExtendedNettyLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
        super(resourceType, samplingInterval, maxActive);
    }

    public ExtendedNettyLeakDetector(Class<?> resourceType, int samplingInterval) {
        super(resourceType, samplingInterval);
    }

    public ExtendedNettyLeakDetector(String resourceType, int samplingInterval, long maxActive) {
        super(resourceType, samplingInterval, maxActive);
    }

    private boolean exitThreadStarted;

    @Override
    protected boolean needReport() {
        return true;
    }

    @Override
    protected void reportTracedLeak(String resourceType, String records) {
        super.reportTracedLeak(resourceType, records);
        dumpToFile(resourceType, records);
        maybeExitJVM();
    }

    @Override
    protected void reportUntracedLeak(String resourceType) {
        super.reportUntracedLeak(resourceType);
        dumpToFile(resourceType, null);
        maybeExitJVM();
    }

    @Override
    protected void reportInstancesLeak(String resourceType) {
        super.reportInstancesLeak(resourceType);
        dumpToFile(resourceType, null);
        maybeExitJVM();
    }

    @Override
    protected Object getInitialHint(String resourceType) {
        String hint = initialHint;
        if (hint != null) {
            return hint;
        }
        return super.getInitialHint(resourceType);
    }

    private synchronized void maybeExitJVM() {
        if (exitThreadStarted || !exitJvmOnLeak) {
            return;
        }
        new Thread(() -> {
            LOG.error("Exiting JVM due to Netty resource leak. Dumped to {}", DUMP_DIR.getAbsolutePath());
            System.err.println("Exiting JVM due to Netty resource leak. Dumped to " + DUMP_DIR.getAbsolutePath());
            triggerLeakDetectionBeforeJVMExit();
            System.err.flush();
            System.out.flush();
            Runtime.getRuntime().halt(1);
        }, ExtendedNettyLeakDetector.class.getSimpleName() + "ExitThread").start();
        exitThreadStarted = true;
    }

    private static void maybeRegisterShutdownHook() {
        if (!exitJvmOnLeak && USE_SHUTDOWN_HOOK && isExtendedNettyLeakDetectorEnabled()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!isEnabled()) {
                    return;
                }
                triggerLeakDetectionBeforeJVMExit();
            }, ExtendedNettyLeakDetector.class.getSimpleName() + "ShutdownHook"));
        }
    }

    private static void triggerLeakDetectionBeforeJVMExit() {
        triggerLeakDetection();
        try {
            Thread.sleep(EXIT_JVM_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        triggerLeakDetection();
    }

    private void dumpToFile(String resourceType, String records) {
        if (MODE_OFF.equals(MODE)) {
            return;
        }
        try {
            if (!DUMP_DIR.exists() && !DUMP_DIR.mkdirs()) {
                LOG.warn("Cannot create NETTY_LEAK_DUMP_DIR={}", DUMP_DIR.getAbsolutePath());
                return;
            }
            String timestampPart = DUMP_TIMESTAMP_FORMATTER.format(ZonedDateTime.now());
            long counter = DUMP_COUNTER.incrementAndGet();
            File dumpFile = new File(DUMP_DIR, "netty_leak_" + timestampPart + "_" + counter + ".txt");
            String prefix = exitJvmOnLeak ? "::error::" : "::warning::";

            StringBuilder report = new StringBuilder();
            if (records != null) {
                report.append(prefix).append("Traced leak detected ").append(resourceType).append('\n');
                report.append(records).append('\n');
            } else {
                report.append(prefix).append("Untraced leak detected ").append(resourceType).append('\n');
            }
            report.append('\n');

            Files.writeString(dumpFile.toPath(), report.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.warn("Failed to write Netty leak report to NETTY_LEAK_DUMP_DIR={}",
                    DUMP_DIR.getAbsolutePath(), e);
        }
    }

    private static String normalizeMode(String mode) {
        if (mode == null) {
            return MODE_REPORT;
        }
        String normalized = mode.trim().toLowerCase(Locale.ROOT);
        if (MODE_REPORT.equals(normalized) || MODE_FAIL_ON_LEAK.equals(normalized) || MODE_OFF.equals(normalized)) {
            return normalized;
        }
        return MODE_REPORT;
    }
}
