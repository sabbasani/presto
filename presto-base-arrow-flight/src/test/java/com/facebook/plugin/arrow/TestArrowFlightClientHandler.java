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

import com.facebook.presto.spi.ConnectorSession;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;

import javax.inject.Inject;

public class TestArrowFlightClientHandler
        extends ArrowFlightClientHandler
{
    @Inject
    public TestArrowFlightClientHandler(ArrowFlightConfig config)
    {
        super(config);
    }

    @Override
    protected CredentialCallOption getCallOptions(ConnectorSession connectorSession)
    {
        return new CredentialCallOption(new BearerCredentialWriter(null));
    }
}
