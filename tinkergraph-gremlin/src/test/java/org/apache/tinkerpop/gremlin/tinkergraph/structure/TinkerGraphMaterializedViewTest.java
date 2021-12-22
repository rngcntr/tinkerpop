/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.MaterializedView;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TinkerGraphMaterializedViewTest {

    @Test
    public void EmptyGraphShouldHaveNoMaterializedViews() {
        final TinkerGraph g = TinkerGraph.open();
        Set<String> mViewKeys = g.getMaterializedViewKeys();
        assertTrue(mViewKeys.isEmpty());
    }

    @Test
    public void RegisteredMaterializedViewIsSavedWithName() {
        final TinkerGraph g = TinkerGraph.open();
        MaterializedView mView = mock(MaterializedView.class);
        g.registerMaterializedView("myView", mView);
        assertEquals(mView, g.materializedView("myView"));
    }
}
