package main

import (
	"fmt"
	"hartomedia-studios/hartodb/library/htdb"
	"time"
)

func main() {
	fmt.Println("Starting HTDB library test")

	// Initialize the database
	db := htdb.NewHTDB("./hartoDB")

	// Create a schema
	schema, err := db.CreateSchema("testSchema")
	if err != nil {
		fmt.Println("Error creating schema:", err)
		return
	}
	fmt.Println("Schema created successfully")

	// Create a table with various field types
	fields := []htdb.Field{
		{Name: "name", Type: "string", Length: 32, Constraints: []htdb.Constraint{htdb.NotNull}},
		{Name: "age", Type: "int", Length: 8, Constraints: []htdb.Constraint{}},
		{Name: "score", Type: "float", Length: 8, Constraints: []htdb.Constraint{}},
		{Name: "description", Type: "ref", Length: 128, Constraints: []htdb.Constraint{}},
	}

	tableResponse := schema.CreateTable("testTable", fields)
	if tableResponse.StatusCode >= 400 {
		fmt.Println("Error creating table:", tableResponse.Message)
		return
	}
	fmt.Println("Table created successfully")

	// Get the table through the table manager
	table, err := db.GetTableManager().GetTable("testSchema", "testTable")
	if err != nil {
		fmt.Println("Error getting table:", err)
		return
	}

	fmt.Println("Table retrieved successfully:", table.TableName)

	// Insert a few records
	fmt.Println("\n=== Inserting records ===")
	records := []map[string]interface{}{
		{
			"name":        "Alice",
			"age":         30,
			"score":       95.5,
			"description": "Senior Software Engineer with 8 years of experience",
		},
		{
			"name":        "Bob",
			"age":         28,
			"score":       87.2,
			"description": "Frontend Developer specializing in React and Vue",
		},
		{
			"name":        "Charlie",
			"age":         35,
			"score":       92.8,
			"description": "DevOps Engineer with expertise in Kubernetes and Docker",
		},
	}

	// Use transactions for inserting records
	tx := db.GetTableManager().BeginTransaction()
	fmt.Println("Transaction started with ID:", tx.ID)

	var insertedRecords []*htdb.Record
	for i, data := range records {
		record, err := tx.StageInsert(table, data)
		if err != nil {
			fmt.Printf("Error staging insert for record %d: %v\n", i+1, err)
			db.GetTableManager().RollbackTransaction(tx)
			return
		}
		insertedRecords = append(insertedRecords, record)
		fmt.Printf("Record %d staged for insertion with ID: %d\n", i+1, record.ID)
	}

	// Commit the transaction

	err = db.GetTableManager().CommitTransaction(tx)
	if err != nil {
		fmt.Println("Error committing transaction:", err)
		return
	}
	fmt.Println("Transaction committed successfully")

	// Read all records
	fmt.Println("\n=== Reading all records ===")
	allRecords, err := db.GetTableManager().GetCurrentRecords(table)
	if err != nil {
		fmt.Println("Error reading records:", err)
		return
	}

	fmt.Printf("Found %d records\n", len(allRecords))
	for i, record := range allRecords {
		fmt.Printf("Record %d:\n", i+1)
		fmt.Printf("  ID: %d\n", record.ID)
		fmt.Printf("  Name: %s\n", record.FieldsData["name"])
		fmt.Printf("  Age: %d\n", record.FieldsData["age"])
		fmt.Printf("  Score: %.1f\n", record.FieldsData["score"])

		// Read the ref field content
		description, err := record.ReadRefData(table.SchemaPath, table.TableName, "description")
		if err != nil {
			fmt.Printf("  Error reading description: %v\n", err)
		} else {
			fmt.Printf("  Description: %s\n", description)
		}
		fmt.Println()
	}

	// Update a record
	if len(allRecords) > 0 {
		fmt.Println("\n=== Updating a record ===")
		recordToUpdate := allRecords[0]

		tx = db.GetTableManager().BeginTransaction()
		updates := map[string]interface{}{
			"score":       99.9,
			"description": "Updated: Senior Software Engineer with 8+ years of experience",
		}

		updatedRecord, err := tx.StageUpdate(table, recordToUpdate, updates)
		if err != nil {
			fmt.Println("Error staging update:", err)
			db.GetTableManager().RollbackTransaction(tx)
			return
		}

		err = db.GetTableManager().CommitTransaction(tx)
		if err != nil {
			fmt.Println("Error committing update transaction:", err)
			return
		}

		fmt.Printf("Record with ID %d updated successfully\n", updatedRecord.ID)

		// Verify the update
		updatedRecords, err := db.GetTableManager().GetCurrentRecords(table)
		if err != nil {
			fmt.Println("Error reading updated records:", err)
			return
		}

		for _, record := range updatedRecords {
			if record.ID == updatedRecord.ID {
				fmt.Println("Updated record:")
				fmt.Printf("  ID: %d\n", record.ID)
				fmt.Printf("  Name: %s\n", record.FieldsData["name"])
				fmt.Printf("  Score: %.1f\n", record.FieldsData["score"])

				description, err := record.ReadRefData(table.SchemaPath, table.TableName, "description")
				if err != nil {
					fmt.Printf("  Error reading description: %v\n", err)
				} else {
					fmt.Printf("  Description: %s\n", description)
				}
				break
			}
		}
	}

	// Delete a record
	if len(allRecords) > 1 {
		fmt.Println("\n=== Deleting a record ===")
		recordToDelete := allRecords[1]

		err = db.GetTableManager().DeleteRecord(table, recordToDelete)
		if err != nil {
			fmt.Println("Error deleting record:", err)
			return
		}

		fmt.Printf("Record with ID %d deleted successfully\n", recordToDelete.ID)

		// Verify the delete
		remainingRecords, err := db.GetTableManager().GetCurrentRecords(table)
		if err != nil {
			fmt.Println("Error reading remaining records:", err)
			return
		}

		fmt.Printf("Remaining records: %d\n", len(remainingRecords))
	}

	// Start the cleanup worker
	fmt.Println("\n=== Starting cleanup worker ===")
	err = db.GetTableManager().StartCleanupWorker(1 * time.Minute)
	if err != nil {
		fmt.Println("Error starting cleanup worker:", err)
		return
	}
	fmt.Println("Cleanup worker started successfully")

	// Stop the cleanup worker after a short period
	time.Sleep(2 * time.Second)
	err = db.GetTableManager().StopCleanupWorker()
	if err != nil {
		fmt.Println("Error stopping cleanup worker:", err)
		return
	}
	fmt.Println("Cleanup worker stopped successfully")

	fmt.Println("\nHTDB library test completed successfully")
}
