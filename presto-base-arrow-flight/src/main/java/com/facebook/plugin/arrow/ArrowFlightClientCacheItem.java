/*
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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_ERROR;

public class ArrowFlightClientCacheItem
{
    private static final Logger logger = Logger.get(ArrowFlightClientCacheItem.class);
    private AtomicBoolean isClientClosed = new AtomicBoolean(true);
    private final ArrowFlightConfig config;
    private ScheduledExecutorService scheduledExecutorService;
    private ArrowFlightClient arrowFlightClient;
    private static final int TIMER_DURATION_IN_MINUTES = 30;
    private RootAllocator allocator;
    private final Optional<String> uri;

    public ArrowFlightClientCacheItem(ArrowFlightConfig config, Optional<String> uri)
    {
        this.config = config;
        this.uri = uri;
    }

    private void initializeClient()
    {
        if (!isClientClosed.get()) {
            return;
        }
        try {
            allocator = new RootAllocator(Long.MAX_VALUE);
            Optional<InputStream> trustedCertificate = Optional.empty();

            Location location;
            if (uri.isPresent()) {
                location = new Location(uri.get());
            }
            else {
                if (config.getArrowFlightServerSslEnabled() != null && !config.getArrowFlightServerSslEnabled()) {
                    location = Location.forGrpcInsecure(config.getFlightServerName(), config.getArrowFlightPort());
                }
                else {
                    location = Location.forGrpcTls(config.getFlightServerName(), config.getArrowFlightPort());
                }
            }

            FlightClient.Builder flightClientBuilder = FlightClient.builder(allocator, location);
            if (config.getVerifyServer() != null && !config.getVerifyServer()) {
                flightClientBuilder.verifyServer(false);
            }
            else if (config.getFlightServerSSLCertificate() != null) {
                trustedCertificate = Optional.of(new FileInputStream(config.getFlightServerSSLCertificate()));
                flightClientBuilder.trustedCertificates(trustedCertificate.get()).useTls();
            }

            FlightClient flightClient = flightClientBuilder.build();
            this.arrowFlightClient = new ArrowFlightClient(flightClient, trustedCertificate, allocator);
            isClientClosed.set(false);
        }
        catch (Exception ex) {
            throw new ArrowException(ARROW_FLIGHT_ERROR, "The flight client could not be obtained." + ex.getMessage(), ex);
        }
    }

    public synchronized ArrowFlightClient getClient()
    {
        if (isClientClosed.get()) { // Check if client is closed or not initialized
            logger.info("Reinitialize the client if closed or not initialized");
            initializeClient();
            scheduleCloseTask();
        }
        else {
            resetTimer(); // Reset timer when client is reused
        }
        return this.arrowFlightClient;
    }

    public synchronized void close() throws Exception
    {
        if (arrowFlightClient != null) {
            arrowFlightClient.close();
            arrowFlightClient = null;
        }
        shutdownTimer();
        isClientClosed.set(true);
    }

    private void scheduleCloseTask()
    {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        Runnable closeTask = () -> {
            try {
                close();
                logger.info("in closeTask");
            }
            catch (Exception e) {
                logger.error(e);
            }
            scheduledExecutorService.shutdown();
        };
        scheduledExecutorService.schedule(closeTask, TIMER_DURATION_IN_MINUTES, TimeUnit.MINUTES);
    }

    public void resetTimer()
    {
        shutdownTimer();
        scheduleCloseTask();
    }

    public void shutdownTimer()
    {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }
}
