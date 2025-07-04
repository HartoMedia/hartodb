# HartoDB

HartoDB is a lightweight, append-only embedded database library written in Go. It supports basic schema and table management, record versioning, transactions, and background cleanup, with a focus on simplicity and extensibility.

---

## Features

- **Schema & Table Management**  
  Create schemas and tables with field type validation.

- **Field Types**  
  Supports `string`, `int`, `float`, `bool`, `timeID`, and `ref` (reference fields for large or external data).

- **Append-Only Storage**  
  Records are never overwritten; versioning and soft-deletes are supported.

- **Transactions**  
  Insert, update, and delete operations are transactional with commit/rollback.

- **Background Cleanup**  
  Periodic worker removes outdated and deleted records to reclaim space.

- **File-Based Persistence**  
  All data is stored in files on disk, with separate files for tables, configs, and reference fields.

- **Standardized Responses**  
  Consistent error and status reporting via a `Response` struct.

---

## Installation

```sh
go get github.com/HartoMedia/hartodb
```

---

## Usage Example

```go
import (
    "hartomedia/hartodb/library/htdb"
    "time"
)

func main() {
    db := htdb.NewHTDB("./hartoDB")

    schema, _ := db.CreateSchema("testSchema")

    fields := []htdb.Field{
        {Name: "name", Type: "string", Length: 32, Constraints: []htdb.Constraint{htdb.NotNull}},
        {Name: "age", Type: "int", Length: 8},
        {Name: "score", Type: "float", Length: 8},
        {Name: "description", Type: "ref", Length: 128},
    }

    schema.CreateTable("testTable", fields)

    table, _ := db.GetTableManager().GetTable("testSchema", "testTable")
    tx := db.GetTableManager().BeginTransaction()

    tx.StageInsert(table, map[string]interface{}{
        "name": "Alice",
        "age": 30,
        "score": 95.5,
        "description": "Senior Engineer",
    })

    db.GetTableManager().CommitTransaction(tx)

    // Query with sorting and limit
    records, _ := db.GetTableManager().Select(table).Sort("age", true).Limit(10).GetAll()

    // Sort in descending order
    highScores, _ := db.GetTableManager().Select(table).Sort("score", false).GetAll()

    // Filter records with Where
    adults, _ := db.GetTableManager().Select(table).Where("age", ">=", 18).GetAll()

    // Chain multiple Where conditions
    qualifiedCandidates, _ := db.GetTableManager().Select(table).
        Where("age", ">=", 25).
        Where("score", ">", 90.0).
        Sort("score", false).
        Limit(5).
        GetAll()

    // Start cleanup worker
    db.GetTableManager().StartCleanupWorker(1 * time.Minute)
}
```

👉 See `library/lib.test.go` for a full-featured example.

---

## Project Structure

```
library/
├── htdb/          # Core library code (schemas, tables, records, transactions, cleanup worker)
└── lib.test.go    # Example usage and test script
```

---

## Roadmap

- [ ] Add indexing and query language
- [ ] Improve concurrency and locking
- [ ] Enhance error handling and documentation
- [ ] Add unit tests and CI/CD integration

---

## Contributing

Contributions are welcome! Please open issues or submit pull requests to improve the Database, fix bugs, or suggest features.
---

## License

This project is licensed under the **MIT License**.  
See the `LICENSE` file for more details.

---

For more details, see the code and comments in the `library/htdb/` directory.
