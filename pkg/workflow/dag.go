package workflow

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Node represents a task in the DAG
type Node struct {
	ID           string
	Dependencies []string
	// Allow attaching payloads like actual Job config or function
	Payload interface{}
}

// DAG represents a Directed Acyclic Graph of jobs
type DAG struct {
	nodes map[string]*Node
	mu    sync.RWMutex
}

// NewDAG creates a new empty DAG
func NewDAG() *DAG {
	return &DAG{
		nodes: make(map[string]*Node),
	}
}

// GetNode retrieves a node by ID
func (d *DAG) GetNode(id string) *Node {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.nodes[id]
}

// AddNode adds a new node to the DAG with optional dependencies
func (d *DAG) AddNode(id string, payload interface{}, deps ...string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.nodes[id]; exists {
		return fmt.Errorf("node %s already exists", id)
	}

	d.nodes[id] = &Node{
		ID:           id,
		Payload:      payload, // Store config/payload
		Dependencies: deps,
	}
	return nil
}

// AddDependency adds a dependency to an existing node
func (d *DAG) AddDependency(id string, dep string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	node, exists := d.nodes[id]
	if !exists {
		return fmt.Errorf("node %s does not exist", id)
	}

	// Avoid duplicate dependencies
	for _, existingDep := range node.Dependencies {
		if existingDep == dep {
			return nil
		}
	}

	node.Dependencies = append(node.Dependencies, dep)
	return nil
}

// GetExecutionPlan returns a slice of layers.
// Each layer contains a list of Node IDs that can be executed in parallel.
// The layers are ordered sequentially.
func (d *DAG) GetExecutionPlan() ([][]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// 1. Calculate in-degrees and build adjacency list
	inDegree := make(map[string]int)
	adj := make(map[string][]string)

	// Initialize
	for id := range d.nodes {
		inDegree[id] = 0
		adj[id] = []string{}
	}

	// Build graph
	for id, node := range d.nodes {
		for _, dep := range node.Dependencies {
			if _, exists := d.nodes[dep]; !exists {
				return nil, fmt.Errorf("node %s depends on missing node %s", id, dep)
			}
			adj[dep] = append(adj[dep], id)
			inDegree[id]++
		}
	}

	// 2. Kahn's Algorithm for Topological Sort (Layered)
	var queue []string

	// Find all nodes with 0 in-degree (roots)
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}

	// Sort queue for deterministic output
	sort.Strings(queue)

	var layers [][]string
	processedCount := 0
	totalCount := len(d.nodes)

	for len(queue) > 0 {
		// The current queue represents a layer of tasks that can run in parallel
		currentLayer := make([]string, len(queue))
		copy(currentLayer, queue)
		layers = append(layers, currentLayer)

		processedCount += len(queue)

		var nextQueue []string

		// Process current layer: reduce in-degree of neighbors
		for _, u := range queue {
			for _, v := range adj[u] {
				inDegree[v]--
				if inDegree[v] == 0 {
					nextQueue = append(nextQueue, v)
				}
			}
		}

		sort.Strings(nextQueue)
		queue = nextQueue
	}

	// 3. Cycle Detection
	if processedCount != totalCount {
		return nil, fmt.Errorf("circular dependency detected in DAG")
	}

	return layers, nil
}

// Validate checks if the DAG is valid (no cycles, all deps exist)
func (d *DAG) Validate() error {
	_, err := d.GetExecutionPlan()
	return err
}

// String returns a string representation of the DAG
func (d *DAG) String() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var sb strings.Builder
	var keys []string
	for k := range d.nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		node := d.nodes[k]
		if len(node.Dependencies) > 0 {
			sb.WriteString(fmt.Sprintf("%s -> [%s]\n", k, strings.Join(node.Dependencies, ", ")))
		} else {
			sb.WriteString(fmt.Sprintf("%s -> (root)\n", k))
		}
	}
	return sb.String()
}
