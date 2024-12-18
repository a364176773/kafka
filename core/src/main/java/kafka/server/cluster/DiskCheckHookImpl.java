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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import kafka.log.LogManager;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskCheckHookImpl implements DiskCheckHook {

	private static final Logger LOGGER = LoggerFactory.getLogger(DiskCheckHookImpl.class);

	private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);

	private static final Map<String, Boolean> DISK_STATUS = new ConcurrentHashMap<>();

    private static volatile BigDecimal THRESHOLD;

    private static final int INTERVAL = Integer.parseInt(System.getProperty("disk.check.interval", "60"));

	private LogManager logManager;

    static {
        String threshold = System.getProperty("disk.threshold", "100");
        try {
            long value = Long.parseLong(threshold);
            if (value < 100L) {
                THRESHOLD = BigDecimal.valueOf(value);
            }
        } catch (NumberFormatException e) {
            LOGGER.error("Failed to parse disk threshold value: {}", threshold, e);
        }
    }

	@Override
    public void beforeDiskCheck(String directory) {
		if (THRESHOLD == null || directory == null || directory.isEmpty()) {
			return;
		}
        Boolean pass = DISK_STATUS.computeIfAbsent(directory, k -> true);
        if (!pass) {
            throw new RecordTooLargeException(
                "Insufficient disk space, stopping write operations to protect the broker");
        }
    }

    @Override
    public void registry(String directory) {
        if (THRESHOLD == null || directory == null || directory.isEmpty()) {
            return;
        }
        Boolean bool = DISK_STATUS.get(directory);
        if (bool == null) {
	        DISK_STATUS.computeIfAbsent(directory, k -> {
                LOGGER.info("Start to monitor disk usage percentage, directory: {}", directory);
                SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(() -> {
                    try {
                        BigDecimal remaining = getDiskSpaceUsagePercentage(directory);
	                    int comparisonResult = remaining.compareTo(THRESHOLD);
                        if (comparisonResult > 0) {
	                        LOGGER.error("directory: {}, Disk usage percentage: {}%, write operations are not allowed", directory, remaining);
                            DISK_STATUS.put(directory, false);
	                        Optional.ofNullable(logManager).ifPresent(LogManager::cleanupLogs);
                        } else {
	                        LOGGER.info("directory: {}, Disk usage percentage: {}%", directory, remaining);
                            DISK_STATUS.put(directory, true);
                        }
                    } catch (IOException e) {
                        LOGGER.error("Failed to get disk usage percentage, directory: {},", directory, e);
                    }
                }, INTERVAL, INTERVAL, TimeUnit.SECONDS);
                return true;
            });
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

	public  void setLogManager(LogManager logManager){
		this.logManager = logManager;
	}

	public static DiskCheckHookImpl getInstance() {
		return InstanceHolder.INSTANCE;
	}

	public static class InstanceHolder {
		private static final DiskCheckHookImpl INSTANCE = new DiskCheckHookImpl();
	}
}