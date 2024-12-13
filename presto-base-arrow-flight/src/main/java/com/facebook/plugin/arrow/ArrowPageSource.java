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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_CLIENT_ERROR;

public class ArrowPageSource
        implements ConnectorPageSource
{
    private static final Logger logger = Logger.get(ArrowPageSource.class);
    private final ArrowSplit split;
    private final List<ArrowColumnHandle> columnHandles;
    private final ArrowBlockBuilder arrowBlockBuilder;
    private boolean completed;
    private int currentPosition;
    private VectorSchemaRoot vectorSchemaRoot;
    private ArrowFlightClient flightClient;
    private FlightStream flightStream;

    public ArrowPageSource(
            ArrowSplit split,
            List<ArrowColumnHandle> columnHandles,
            ArrowFlightClientHandler clientHandler,
            ConnectorSession connectorSession,
            ArrowBlockBuilder arrowBlockBuilder)
    {
        this.columnHandles = columnHandles;
        this.split = split;
        this.arrowBlockBuilder = arrowBlockBuilder;
        getFlightStream(clientHandler, split.getTicket(), connectorSession);
    }

    private void getFlightStream(ArrowFlightClientHandler clientHandler, byte[] ticket, ConnectorSession connectorSession)
    {
        try {
            Optional<String> uri = (split.getLocationUrls().isEmpty()) ?
                    Optional.empty() : Optional.of(split.getLocationUrls().get(0));
            flightClient = clientHandler.getClient(uri);
            flightStream = flightClient.getFlightClient().getStream(new Ticket(ticket), clientHandler.getCallOptions(connectorSession));
        }
        catch (FlightRuntimeException e) {
            throw new ArrowException(ARROW_FLIGHT_CLIENT_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedPositions()
    {
        return currentPosition;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return completed;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public Page getNextPage()
    {
        if (flightStream.next()) {
            vectorSchemaRoot = flightStream.getRoot();
        }
        else {
            completed = true;
            return null;
        }

        currentPosition = currentPosition + 1;

        List<Block> blocks = new ArrayList<>();
        for (int columnIndex = 0; columnIndex < columnHandles.size(); columnIndex++) {
            FieldVector vector = vectorSchemaRoot.getVector(columnIndex);
            Type type = columnHandles.get(columnIndex).getColumnType();
            Block block = arrowBlockBuilder.buildBlockFromFieldVector(vector, type, flightStream.getDictionaryProvider());
            blocks.add(block);
        }

        return new Page(vectorSchemaRoot.getRowCount(), blocks.toArray(new Block[0]));
    }

    @Override
    public void close()
    {
        if (vectorSchemaRoot != null) {
            vectorSchemaRoot.close();
            completed = true;
        }

        if (flightStream != null) {
            try {
                flightStream.close();
            }
            catch (Exception e) {
                logger.error(e);
            }
        }
        try {
            if (flightClient != null) {
                flightClient.close();
                flightClient = null;
            }
        }
        catch (Exception ex) {
            logger.error("Failed to close the flight client: %s", ex.getMessage(), ex);
        }
    }
}
