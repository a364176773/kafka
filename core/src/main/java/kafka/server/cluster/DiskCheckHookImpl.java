/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.cluster;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskCheckHookImpl implements DiskCheckHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiskCheckHookImpl.class);

    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);

    private static final Map<String, BigDecimal> DISK_FREE_PERCENTAGE = new ConcurrentHashMap<>();

    private static final BigDecimal THRESHOLD =
        BigDecimal.valueOf(Long.parseLong(System.getProperty("disk.threshold", "100")));

    @Override
    public void beforeDiskCheck(String directory) {
        if (directory == null || directory.isEmpty()) {
            return;
        }
        BigDecimal bigDecimal = DISK_FREE_PERCENTAGE.computeIfAbsent(directory, k -> {
            SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(() -> {
                try {
                    BigDecimal remaining = getDiskSpaceUsagePercentage(directory);
                    LOGGER.info("directory: {}, Disk usage percentage: {}%", directory,remaining);
                    DISK_FREE_PERCENTAGE.put(directory, remaining);
                } catch (IOException e) {
                    LOGGER.error("Failed to get disk usage percentage, directory: {},", directory, e);
                }
            }, 0, 1, TimeUnit.MINUTES);
            return BigDecimal.valueOf(0);
        });
        int comparisonResult = bigDecimal.compareTo(THRESHOLD);
        if (comparisonResult > 0) {
            throw new RecordTooLargeException("Disk usage is above the threshold: " + bigDecimal + "%");
        }
    }

    public BigDecimal getDiskSpaceUsagePercentage(String directory) throws IOException {
        FileStore fileStore = Files.getFileStore(Paths.get(directory));
        long totalSpace = fileStore.getTotalSpace();
        long usableSpace = fileStore.getUsableSpace();
        long usedSpace = totalSpace - usableSpace;
        return BigDecimal.valueOf(usedSpace).multiply(BigDecimal.valueOf(100)).divide(BigDecimal.valueOf(totalSpace), 2,
            RoundingMode.HALF_UP);
    }

}
