use std::collections::{HashMap, HashSet, VecDeque};
use std::time::SystemTime;
use crate::transaction::Transaction;
use crate::storage::Storage;
use crate::indexes::{index_manager::IndexManager};
use crate::fts::search::Search;
use crate::fts::default::DefaultSearchIdx;
use crate::storage::memory::InMemoryStorage;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct WaitForEdge {
    from_tx: u64,
    to_tx: u64,
    resource: String,
}

pub struct DeadlockDetector {
    wait_for_graph: HashMap<u64, HashSet<WaitForEdge>>,
}

impl DeadlockDetector {
    pub fn new() -> Self {
        DeadlockDetector {
            wait_for_graph: HashMap::new(),
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

    pub fn detect_deadlock<S, FTS>(&self, transactions: &[&Transaction<S, FTS>]) -> Option<u64>
    where
        S: Storage + IndexManager + Clone,
        FTS: Search + Clone,
        FTS::NewArgs: Clone,
    {
        for &start_tx in self.wait_for_graph.keys() {
            if let Some(cycle) = self.find_cycle(start_tx) {
                // Return the youngest transaction in the cycle
                return Some(self.select_victim(&cycle, transactions));
            }
        }
        None
    }

    fn find_cycle(&self, start_tx: u64) -> Option<Vec<u64>> {
        let mut visited = HashSet::new();
        let mut path = Vec::new();
        
        // Check for self-deadlock first
        if let Some(edges) = self.wait_for_graph.get(&start_tx) {
            if edges.iter().any(|edge| edge.to_tx == start_tx) {
                return Some(vec![start_tx]);
            }
        }
        
        // Check for other cycles
        self.dfs_find_cycle(start_tx, start_tx, &mut visited, &mut path)
    }

    fn dfs_find_cycle(&self, current_tx: u64, start_tx: u64, visited: &mut HashSet<u64>, path: &mut Vec<u64>) -> Option<Vec<u64>> {
        path.push(current_tx);
        visited.insert(current_tx);

        if let Some(edges) = self.wait_for_graph.get(&current_tx) {
            for edge in edges {
                if edge.to_tx == start_tx && path.len() > 1 {
                    // Found a cycle back to start
                    return Some(path.clone());
                }
                if !visited.contains(&edge.to_tx) {
                    if let Some(mut cycle) = self.dfs_find_cycle(edge.to_tx, start_tx, visited, path) {
                        return Some(cycle);
                    }
                }
            }
        }

        path.pop();
        visited.remove(&current_tx);
        None
    }

    fn select_victim<S, FTS>(&self, cycle: &[u64], transactions: &[&Transaction<S, FTS>]) -> u64 
    where 
        S: Storage + IndexManager + Clone,
        FTS: Search + Clone,
        FTS::NewArgs: Clone,
    {
        // Select the transaction with the latest timestamp (youngest)
        cycle.iter()
            .max_by_key(|&&tx_id| {
                transactions.iter()
                    .find(|t| t.get_id() == tx_id)
                    .map(|t| t.get_start_timestamp())
                    .unwrap_or(SystemTime::UNIX_EPOCH)
            })
            .copied()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryReefDB;
    use crate::transaction::IsolationLevel;

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
        let db = InMemoryReefDB::create_in_memory().unwrap();
        
        // Create transactions
        let tx1 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let tx2 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let transactions: Vec<&Transaction<InMemoryStorage, DefaultSearchIdx>> = vec![&tx1, &tx2];
        
        // Create a simple deadlock: T1 -> T2 -> T1
        detector.add_wait(tx1.get_id(), tx2.get_id(), "users".to_string());
        detector.add_wait(tx2.get_id(), tx1.get_id(), "posts".to_string());
        
        // Detect deadlock
        let victim = detector.detect_deadlock(&transactions);
        assert!(victim.is_some());
        assert_eq!(victim.unwrap(), tx2.get_id()); // Should select transaction 2 as victim (latest timestamp)
    }

    #[test]
    fn test_detect_deadlock_complex() {
        let mut detector = DeadlockDetector::new();
        let db = InMemoryReefDB::create_in_memory().unwrap();
        
        // Create transactions with delays to ensure proper timestamp ordering
        let tx1 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let tx2 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let tx3 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let tx4 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let transactions: Vec<&Transaction<InMemoryStorage, DefaultSearchIdx>> = vec![&tx1, &tx2, &tx3, &tx4];
        
        // Create a more complex deadlock:
        // T1 -> T2 -> T3 -> T4 -> T2
        detector.add_wait(tx1.get_id(), tx2.get_id(), "table1".to_string());
        detector.add_wait(tx2.get_id(), tx3.get_id(), "table2".to_string());
        detector.add_wait(tx3.get_id(), tx4.get_id(), "table3".to_string());
        detector.add_wait(tx4.get_id(), tx2.get_id(), "table4".to_string());
        
        // Detect deadlock
        let victim = detector.detect_deadlock(&transactions);
        
        assert!(victim.is_some());
        assert_eq!(victim.unwrap(), tx4.get_id(), 
            "Expected tx4 to be selected as victim (latest timestamp)");
    }

    #[test]
    fn test_no_deadlock() {
        let mut detector = DeadlockDetector::new();
        let db = InMemoryReefDB::create_in_memory().unwrap();
        
        // Create transactions
        let tx1 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let tx2 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let tx3 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let transactions: Vec<&Transaction<InMemoryStorage, DefaultSearchIdx>> = vec![&tx1, &tx2, &tx3];
        
        // Create a wait-for graph without cycles:
        // T1 -> T2 -> T3
        detector.add_wait(tx1.get_id(), tx2.get_id(), "users".to_string());
        detector.add_wait(tx2.get_id(), tx3.get_id(), "posts".to_string());
        
        // Should not detect any deadlock
        let victim = detector.detect_deadlock(&transactions);
        assert!(victim.is_none());
    }

    #[test]
    fn test_multiple_edges() {
        let mut detector = DeadlockDetector::new();
        let db = InMemoryReefDB::create_in_memory().unwrap();
        
        // Create transactions
        let tx1 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let tx2 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let tx3 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let tx4 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let transactions: Vec<&Transaction<InMemoryStorage, DefaultSearchIdx>> = vec![&tx1, &tx2, &tx3, &tx4];
        
        // Add multiple edges for the same transaction
        detector.add_wait(tx1.get_id(), tx2.get_id(), "users".to_string());
        detector.add_wait(tx1.get_id(), tx3.get_id(), "posts".to_string());
        detector.add_wait(tx1.get_id(), tx4.get_id(), "comments".to_string());
        
        assert_eq!(detector.wait_for_graph.len(), 1);
        assert_eq!(detector.wait_for_graph.get(&tx1.get_id()).unwrap().len(), 3);
        
        // No deadlock should be detected
        assert!(detector.detect_deadlock(&transactions).is_none());
    }

    #[test]
    fn test_self_deadlock() {
        let mut detector = DeadlockDetector::new();
        let db = InMemoryReefDB::create_in_memory().unwrap();
        
        // Create transaction
        let tx1 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        let transactions: Vec<&Transaction<InMemoryStorage, DefaultSearchIdx>> = vec![&tx1];
        
        // Create a self-deadlock: T1 -> T1
        detector.add_wait(tx1.get_id(), tx1.get_id(), "users".to_string());
        
        // Should detect the self-deadlock
        let victim = detector.detect_deadlock(&transactions);
        assert!(victim.is_some());
        assert_eq!(victim.unwrap(), tx1.get_id());
    }

    #[test]
    fn test_select_victim() {
        let detector = DeadlockDetector::new();
        let db = InMemoryReefDB::create_in_memory().unwrap();

        // Create transactions with different timestamps
        let tx1 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        std::thread::sleep(std::time::Duration::from_millis(10)); // Ensure different timestamps
        let tx2 = Transaction::create(db.clone(), IsolationLevel::Serializable);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let tx3 = Transaction::create(db.clone(), IsolationLevel::Serializable);

        let cycle = vec![tx1.get_id(), tx2.get_id(), tx3.get_id()];
        let transactions: Vec<&Transaction<InMemoryStorage, DefaultSearchIdx>> = vec![&tx1, &tx2, &tx3];

        // Should select tx3 as victim since it has the latest timestamp
        let victim = detector.select_victim(&cycle, &transactions);
        assert_eq!(victim, tx3.get_id());
    }
} 