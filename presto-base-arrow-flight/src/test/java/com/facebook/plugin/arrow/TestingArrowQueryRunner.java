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

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestingArrowQueryRunner
{
    private TestingArrowQueryRunner() {}

    public static LocalQueryRunner createQueryRunner()
    {
        return createQueryRunner(ImmutableMap.of(), TestingArrowFactory.class);
    }

    public static LocalQueryRunner createQueryRunner(Map<String, String> catalogProperties, Class<? extends ArrowConnectorFactory> factoryClass)
    {
        Session session = testSessionBuilder()
                .setCatalog("arrow")
                .setSchema("testdb")
                .build();

        LocalQueryRunner queryRunner = new LocalQueryRunner(session);

        try {
            ArrowConnectorFactory connectorFactory = new ArrowConnectorFactory(factoryClass.getSimpleName(), new TestingArrowModule(), TestingArrowQueryRunner.class.getClassLoader());
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .putAll(catalogProperties)
                    .put("arrow-flight.server", "127.0.0.1")
                    .put("arrow-flight.server-ssl-enabled", "true")
                    .put("arrow-flight.server.port", "9443")
                    .put("arrow-flight.server.verify", "false")
                    .put("data-source.port", "8443")
                    .put("data-source.host", "9.30.120.2")
                    .put("data-source.database", "db")
                    .put("data-source.username", "user")
                    .put("data-source.password", "password")
                    .put("data-source.ssl", "true");
            queryRunner.createCatalog("arrow", connectorFactory, properties.build());

            return queryRunner;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to create ArrowQueryRunner", e);
        }
    }
}
