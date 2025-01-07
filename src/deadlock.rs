use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicU64;
use crate::error::ReefDBError;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct WaitForEdge {
    from_tx: u64,
    to_tx: u64,
    resource: String,
}

pub struct DeadlockDetector {
    wait_for_graph: HashMap<u64, HashSet<WaitForEdge>>,
    timestamp_counter: AtomicU64,
}

impl DeadlockDetector {
    pub fn new() -> Self {
        DeadlockDetector {
            wait_for_graph: HashMap::new(),
            timestamp_counter: AtomicU64::new(0),
        }
    }

    pub fn add_wait(&mut self, waiting_tx: u64, holding_tx: u64, resource: String) {
        let edge = WaitForEdge {
            from_tx: waiting_tx,
            to_tx: holding_tx,
            resource,
        };
        self.wait_for_graph
            .entry(waiting_tx)
            .or_insert_with(HashSet::new)
            .insert(edge);
    }

    pub fn remove_transaction(&mut self, tx_id: u64) {
        self.wait_for_graph.remove(&tx_id);
        for edges in self.wait_for_graph.values_mut() {
            edges.retain(|edge| edge.to_tx != tx_id);
        }
    }

    pub fn detect_deadlock(&self) -> Option<u64> {
        for &start_tx in self.wait_for_graph.keys() {
            if let Some(cycle) = self.find_cycle(start_tx) {
                // Return the youngest transaction in the cycle
                return Some(self.select_victim(&cycle));
            }
        }
        None
    }

    fn find_cycle(&self, start_tx: u64) -> Option<Vec<u64>> {
        let mut visited = HashSet::new();
        let mut path = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back(start_tx);

        while let Some(tx) = queue.pop_front() {
            if let Some(edges) = self.wait_for_graph.get(&tx) {
                for edge in edges {
                    if edge.to_tx == start_tx && !path.is_empty() {
                        path.push(tx);
                        return Some(path);
                    }
                    if visited.insert(edge.to_tx) {
                        path.push(tx);
                        queue.push_back(edge.to_tx);
                    }
                }
            }
        }
        None
    }

    fn select_victim(&self, cycle: &[u64]) -> u64 {
        // Select the transaction with the highest timestamp (youngest)
        *cycle.iter().max().unwrap_or(&0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deadlock_detector_new() {
        let detector = DeadlockDetector::new();
        assert!(detector.wait_for_graph.is_empty());
    }

    #[test]
    fn test_add_wait() {
        let mut detector = DeadlockDetector::new();
        detector.add_wait(1, 2, "users".to_string());
        
        assert_eq!(detector.wait_for_graph.len(), 1);
        assert!(detector.wait_for_graph.contains_key(&1));
        
        let edges = detector.wait_for_graph.get(&1).unwrap();
        assert_eq!(edges.len(), 1);
        
        let edge = edges.iter().next().unwrap();
        assert_eq!(edge.from_tx, 1);
        assert_eq!(edge.to_tx, 2);
        assert_eq!(edge.resource, "users");
    }

    #[test]
    fn test_remove_transaction() {
        let mut detector = DeadlockDetector::new();
        
        // Add some wait edges
        detector.add_wait(1, 2, "users".to_string());
        detector.add_wait(2, 3, "posts".to_string());
        detector.add_wait(3, 1, "comments".to_string());
        
        // Remove transaction 2
        detector.remove_transaction(2);
        
        // Check that transaction 2's edges are removed
        assert!(!detector.wait_for_graph.contains_key(&2));
        
        // Check that edges pointing to transaction 2 are removed
        let edges = detector.wait_for_graph.get(&1).unwrap();
        assert!(edges.iter().all(|e| e.to_tx != 2));
    }

    #[test]
    fn test_detect_deadlock_simple() {
        let mut detector = DeadlockDetector::new();
        
        // Create a simple deadlock: T1 -> T2 -> T1
        detector.add_wait(1, 2, "users".to_string());
        detector.add_wait(2, 1, "posts".to_string());
        
        // Detect deadlock
        let victim = detector.detect_deadlock();
        assert!(victim.is_some());
        assert_eq!(victim.unwrap(), 2); // Should select transaction 2 as victim (highest ID)
    }

    #[test]
    fn test_detect_deadlock_complex() {
        let mut detector = DeadlockDetector::new();
        
        // Create a more complex deadlock:
        // T1 -> T2 -> T3 -> T4 -> T2
        detector.add_wait(1, 2, "table1".to_string());
        detector.add_wait(2, 3, "table2".to_string());
        detector.add_wait(3, 4, "table3".to_string());
        detector.add_wait(4, 2, "table4".to_string());
        
        // Detect deadlock
        let victim = detector.detect_deadlock();
        assert!(victim.is_some());
        assert_eq!(victim.unwrap(), 4); // Should select transaction 4 as victim (highest ID in cycle)
    }

    #[test]
    fn test_no_deadlock() {
        let mut detector = DeadlockDetector::new();
        
        // Create a wait-for graph without cycles:
        // T1 -> T2 -> T3
        detector.add_wait(1, 2, "users".to_string());
        detector.add_wait(2, 3, "posts".to_string());
        
        // Should not detect any deadlock
        let victim = detector.detect_deadlock();
        assert!(victim.is_none());
    }

    #[test]
    fn test_multiple_edges() {
        let mut detector = DeadlockDetector::new();
        
        // Add multiple edges for the same transaction
        detector.add_wait(1, 2, "users".to_string());
        detector.add_wait(1, 3, "posts".to_string());
        detector.add_wait(1, 4, "comments".to_string());
        
        assert_eq!(detector.wait_for_graph.len(), 1);
        assert_eq!(detector.wait_for_graph.get(&1).unwrap().len(), 3);
        
        // No deadlock should be detected
        assert!(detector.detect_deadlock().is_none());
    }

    #[test]
    fn test_self_deadlock() {
        let mut detector = DeadlockDetector::new();
        
        // Create a self-deadlock: T1 -> T1
        detector.add_wait(1, 1, "users".to_string());
        
        // Should detect the self-deadlock
        let victim = detector.detect_deadlock();
        assert!(victim.is_some());
        assert_eq!(victim.unwrap(), 1);
    }
} 