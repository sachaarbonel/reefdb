#[cfg(test)]
mod tests {
    use crate::{
        sql::{
            statements::Statement,
            data_value::DataValue,
        },
        InMemoryReefDB,
        transaction::IsolationLevel,
        result::ReefDBResult,
    };

    #[test]
    fn test_mvcc_concurrent_transactions() -> Result<(), crate::error::ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;

        println!("\n=== Setup Transaction ===");
        // Create table in a setup transaction
        let setup_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;
        let create_stmt = Statement::parse("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)").unwrap().1;
        println!("Executing CREATE TABLE statement");
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx, create_stmt)?;
        let insert_stmt = Statement::parse("INSERT INTO accounts VALUES (1, 1000)").unwrap().1;
        println!("Executing INSERT statement");
        db.transaction_manager.as_mut().unwrap().execute_statement(setup_tx, insert_stmt)?;
        println!("Committing setup transaction");
        db.transaction_manager.as_mut().unwrap().commit_transaction(setup_tx)?;

        println!("\n=== Starting Concurrent Transactions ===");
        let tx1_id = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::ReadCommitted)?;
        let tx2_id = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::ReadCommitted)?;
        println!("Started transactions - tx1_id: {}, tx2_id: {}", tx1_id, tx2_id);

        // Transaction 1: Deduct 100 from account 1
        println!("\n=== Transaction 1: Update Balance ===");
        let update_stmt = Statement::parse("UPDATE accounts SET balance = 900 WHERE id = 1").unwrap().1;
        let update_result = db.transaction_manager.as_mut().unwrap().execute_statement(tx1_id, update_stmt)?;
        println!("Update result from tx1: {:?}", update_result);

        // Transaction 2: Read balance before tx1 commits
        println!("\n=== Transaction 2: First Read (Before Commit) ===");
        let select_stmt = Statement::parse("SELECT balance FROM accounts WHERE id = 1").unwrap().1;
        let balance_before_commit = db.transaction_manager.as_mut().unwrap().execute_statement(tx2_id, select_stmt.clone())?;
        println!("Before tx1 commit - Transaction 2 sees balance: {:?}", balance_before_commit);
        match &balance_before_commit {
            crate::result::ReefDBResult::Select(rows) => {
                println!("Number of rows returned: {}", rows.len());
                for (i, row) in rows.iter().enumerate() {
                    println!("Row {}: {:?}", i, row);
                }
            },
            _ => println!("Not a Select result: {:?}", balance_before_commit),
        }

        // Commit transaction 1
        println!("\n=== Committing Transaction 1 ===");
        db.transaction_manager.as_mut().unwrap().commit_transaction(tx1_id)?;
        println!("Transaction 1 committed successfully");

        // Transaction 2: Read balance after tx1 commits
        println!("\n=== Transaction 2: Second Read (After Commit) ===");
        let balance_after_commit = db.transaction_manager.as_mut().unwrap().execute_statement(tx2_id, select_stmt.clone())?;
        println!("After tx1 commit - Transaction 2 sees balance: {:?}", balance_after_commit);
        match &balance_after_commit {
            crate::result::ReefDBResult::Select(rows) => {
                println!("Number of rows returned: {}", rows.len());
                for (i, row) in rows.iter().enumerate() {
                    println!("Row {}: {:?}", i, row);
                }
            },
            _ => println!("Not a Select result: {:?}", balance_after_commit),
        }

        // Commit transaction 2
        println!("\n=== Committing Transaction 2 ===");
        db.transaction_manager.as_mut().unwrap().commit_transaction(tx2_id)?;
        println!("Transaction 2 committed successfully");

        // Verify final balance in a new transaction
        println!("\n=== Final Verification ===");
        let verify_tx = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::ReadCommitted)?;
        let final_balance = db.transaction_manager.as_mut().unwrap().execute_statement(verify_tx, select_stmt)?;
        println!("Final verification - Balance: {:?}", final_balance);
        match &final_balance {
            crate::result::ReefDBResult::Select(rows) => {
                println!("Number of rows returned: {}", rows.len());
                for (i, row) in rows.iter().enumerate() {
                    println!("Row {}: {:?}", i, row);
                }
                assert_eq!(rows[0].1[0], DataValue::Integer(900), "Final balance should be 900 after update");
            },
            _ => panic!("Expected Select result"),
        }
        db.transaction_manager.as_mut().unwrap().commit_transaction(verify_tx)?;

        Ok(())
    }

    #[test]
    fn test_isolation_levels() -> Result<(), crate::error::ReefDBError> {
        let mut db = InMemoryReefDB::create_in_memory()?;
        
        // Create table in initial transaction
        let tx1 = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(tx1, Statement::parse("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().execute_statement(tx1, Statement::parse("INSERT INTO accounts VALUES (1, 100)").unwrap().1)?;
        db.transaction_manager.as_mut().unwrap().commit_transaction(tx1)?;

        // Start two concurrent transactions with different isolation levels
        let tx_serializable = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::Serializable)?;
        let tx_read_committed = db.transaction_manager.as_mut().unwrap().begin_transaction(IsolationLevel::ReadCommitted)?;

        // Update in serializable transaction
        db.transaction_manager.as_mut().unwrap().execute_statement(tx_serializable, 
            Statement::parse("UPDATE accounts SET balance = 200 WHERE id = 1").unwrap().1)?;

        // Read in read committed transaction (should not see uncommitted changes)
        let result = db.transaction_manager.as_mut().unwrap().execute_statement(tx_read_committed,
            Statement::parse("SELECT balance FROM accounts WHERE id = 1").unwrap().1)?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows[0].1[0], DataValue::Integer(100));
        } else {
            panic!("Expected Select result");
        }

        // Commit serializable transaction
        db.transaction_manager.as_mut().unwrap().commit_transaction(tx_serializable)?;

        // Read again in read committed transaction (should now see changes)
        let result = db.transaction_manager.as_mut().unwrap().execute_statement(tx_read_committed,
            Statement::parse("SELECT balance FROM accounts WHERE id = 1").unwrap().1)?;
        
        if let ReefDBResult::Select(rows) = result {
            assert_eq!(rows[0].1[0], DataValue::Integer(200));
        } else {
            panic!("Expected Select result");
        }

        db.transaction_manager.as_mut().unwrap().commit_transaction(tx_read_committed)?;
        
        Ok(())
    }
} 