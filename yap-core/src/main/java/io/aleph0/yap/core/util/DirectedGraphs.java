/*-
 * =================================LICENSE_START==================================
 * yap-core
 * ====================================SECTION=====================================
 * Copyright (C) 2025 aleph0
 * ====================================SECTION=====================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ==================================LICENSE_END===================================
 */
package io.aleph0.yap.core.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

public final class DirectedGraphs {
  private DirectedGraphs() {}

  /**
   * Checks if the given directed graph is weakly connected.
   *
   * <p>
   * A directed graph is weakly connected if there is a path between any two nodes when ignoring the
   * direction of the edges.
   *
   * @param graph the directed graph represented as an adjacency list. Each key is a node, and the
   *        value is a set of nodes that point to it.
   * @return true if the graph is weakly connected, false otherwise
   */
  public static boolean isWeaklyConnected(Map<String, Set<String>> graph) {
    final Set<String> visited = new HashSet<>();
    final Queue<String> queue = new ArrayDeque<>();
    final String startNode = graph.keySet().iterator().next();

    queue.add(startNode);
    visited.add(startNode);

    while (!queue.isEmpty()) {
      final String node = queue.poll();

      // Add all neighbors in the original direction
      for (final String neighbor : graph.getOrDefault(node, Set.of())) {
        if (!visited.contains(neighbor)) {
          visited.add(neighbor);
          queue.offer(neighbor);
        }
      }

      // Add all neighbors in the reverse direction
      for (final Map.Entry<String, Set<String>> entry : graph.entrySet()) {
        if (!entry.getValue().contains(node))
          continue;
        final String neighbor = entry.getKey();
        if (!visited.contains(neighbor)) {
          visited.add(neighbor);
          queue.offer(neighbor);
        }
      }
    }

    return visited.size() == graph.size();
  }

  /**
   * Finds some cycle in the given directed graph, if any exist. Otherwise, returns empty. Only ones
   * cycle is returned, even if multiple cycles exist. There is no guarantee made regarding which
   * cycle is returned, only that one is returned if any exist.
   * 
   * @param graph the directed graph represented as an adjacency list. Each key is a node, and the
   *        value is a set of nodes that point to it.
   * @return an optional containing a list of nodes in the cycle if one exists, or an empty optional
   *         if no cycle exists
   */
  public static Optional<List<String>> findCycle(Map<String, Set<String>> graph) {
    final Set<String> visited = new HashSet<>();
    final Set<String> stack = new HashSet<>();
    final List<String> cycle = new ArrayList<>();

    for (final String node : graph.keySet()) {
      if (findCycleUtil(graph, node, visited, stack, cycle)) {
        return Optional.of(cycle);
      }
    }

    return Optional.empty();
  }

  private static boolean findCycleUtil(Map<String, Set<String>> graph, String node,
      Set<String> visited, Set<String> stack, List<String> cycle) {
    if (stack.contains(node)) {
      cycle.add(node);
      return true;
    }

    if (visited.contains(node)) {
      return false;
    }

    visited.add(node);
    stack.add(node);

    for (final String neighbor : graph.getOrDefault(node, Set.of())) {
      if (findCycleUtil(graph, neighbor, visited, stack, cycle)) {
        if (!cycle.contains(node)) {
          cycle.add(node);
        }
        return true;
      }
    }

    stack.remove(node);
    return false;
  }
}
