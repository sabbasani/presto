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
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public abstract class AbstractArrowSplitManager
        implements ConnectorSplitManager
{
    private static final Logger logger = Logger.get(AbstractArrowSplitManager.class);
    private final ArrowFlightClientHandler clientHandler;
    private final ArrowFlightConfig config;

    public AbstractArrowSplitManager(ArrowFlightClientHandler client, ArrowFlightConfig config)
    {
        this.clientHandler = client;
        this.config = config;
    }

    protected abstract FlightDescriptor getFlightDescriptor(ArrowFlightConfig config, ArrowTableLayoutHandle tableLayoutHandle);

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext)
    {
        ArrowTableLayoutHandle tableLayoutHandle = (ArrowTableLayoutHandle) layout;
        ArrowTableHandle tableHandle = tableLayoutHandle.getTableHandle();
        FlightDescriptor flightDescriptor = getFlightDescriptor(config, tableLayoutHandle);

        FlightInfo flightInfo = clientHandler.getFlightInfo(flightDescriptor, session);
        List<ArrowSplit> splits = flightInfo.getEndpoints()
                .stream()
                .map(info -> new ArrowSplit(
                        tableHandle.getSchema(),
                        tableHandle.getTable(),
                        info.getTicket().getBytes(),
                        info.getLocations().stream().map(location -> location.getUri().toString()).collect(toImmutableList())))
                .collect(toImmutableList());
        return new FixedSplitSource(splits);
    }
}