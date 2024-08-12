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
import com.facebook.presto.spi.ConnectorSession;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.grpc.CredentialCallOption;

import java.util.HashMap;
import java.util.Optional;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_ERROR;

public abstract class ArrowFlightClientHandler
{
    private static final Logger logger = Logger.get(ArrowFlightClientHandler.class);
    private final ArrowFlightConfig config;
    private final HashMap<String, ArrowFlightClientCacheItem> arrowFlightClientCache;

    public ArrowFlightClientHandler(ArrowFlightConfig config)
    {
        this.config = config;
        this.arrowFlightClientCache = new HashMap<>();
    }

    public ArrowFlightConfig getConfig()
    {
        return config;
    }

    public synchronized ArrowFlightClient getClient(Optional<String> uri)
    {
        String cacheKey = uri.orElse(null);
        if (!arrowFlightClientCache.containsKey(cacheKey)) {
            arrowFlightClientCache.put(cacheKey, new ArrowFlightClientCacheItem(config, uri));
        }

        return arrowFlightClientCache.get(cacheKey).getClient();
    }

    public FlightInfo getFlightInfo(ArrowFlightRequest request, ConnectorSession connectorSession)
    {
        try {
            ArrowFlightClient client = getClient(Optional.empty());
            CredentialCallOption auth = this.getCallOptions(connectorSession);
            FlightDescriptor descriptor = FlightDescriptor.command(request.getCommand());
            logger.debug("Fetching flight info");
            FlightInfo flightInfo = client.getFlightClient().getInfo(descriptor, auth);
            logger.debug("got flight info");
            return flightInfo;
        }
        catch (Exception e) {
            throw new ArrowException(ARROW_FLIGHT_ERROR, "The flight information could not be obtained from the flight server." + e.getMessage(), e);
        }
    }
    protected abstract CredentialCallOption getCallOptions(ConnectorSession connectorSession);
}
