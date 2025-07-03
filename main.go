package main

import (
	"encoding/json"
	"fmt"
	"hartomedia-studios/hartodb/library/htdb"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

/* main vars*/

/* errors */

var lastTimestamp int64

type Constraint string

const Uint128Length = 16 // 128 bits - 16 bytes (two uint64s)

const (
	PrimaryKey Constraint = "primary_key"
	NotNull    Constraint = "not_null"
	Unique     Constraint = "unique"
)

type response struct {
	time string
	num  int
	err  string
}

type table struct {
	TableName string  `json:"tableName"`
	CreatedAt string  `json:"createdAt"`
	Fields    []field `json:"fields"`
}

type field struct {
	Name        string       `json:"name"`
	Type        string       `json:"type"`
	Length      uint         `json:"length,omitempty"`
	Constraints []Constraint `json:"constraints"`
}

var timePKField = field{
	Name:        "id",
	Type:        "timeID",
	Length:      8, // 64 bits - 8 bytes (uint64) stored for Nanoseconds since Unix epoch +- 584 years
	Constraints: []Constraint{PrimaryKey, NotNull, Unique},
}

var mainPath = "./hartoDB"

const fileEnding string = ".htdb"

func main() {

	db := htdb.NewHTDB(mainPath)
	fmt.Println(db)

	fmt.Println(createDatabase("testDB1"))

	// Create tables
	fmt.Println(createTable("persons", "", []field{
		{"name", "string", 16, []Constraint{"not_null"}},
		{"age", "int", 8, []Constraint{}},
	}))

	fmt.Println(createTable("", "testDB1", []field{
		{"name", "string", 16, []Constraint{"not_null"}},
		{"age", "int", 8, []Constraint{}},
	}))

	fmt.Println(createTable(".Einkaufsliste", "testDB1", []field{
		{"product", "string", 16, []Constraint{"not_null"}},
		{"cnt", "int", 8, []Constraint{}},
		{"price", "float", 8, []Constraint{}},
	}))

	fmt.Println(createTable("tableData", "testDB1", []field{
		{"name", "string", 16, []Constraint{"not_null"}},
		{"age", "int", 8, []Constraint{}},
		{Name: "place", Type: "ref", Length: Uint128Length, Constraints: []Constraint{NotNull}},
	}))

	var variableTableNameWithTime = "tableData" + time.Now().Format("20060102_150405")
	fmt.Println(createTable(variableTableNameWithTime, "testDB1", []field{
		{"name", "string", 16, []Constraint{"not_null"}},
		{"age", "int", 8, []Constraint{}},
		{"city", "string", 16, []Constraint{}},
		{"longText", "ref", Uint128Length, []Constraint{}},
	}))

	fmt.Println("Create 1")
	data1 := map[string]interface{}{
		"name": "0123456789ABCDEF",
		"age":  30,
	}
	fmt.Println(addToTable("testDB1", variableTableNameWithTime, data1))

	fmt.Println("Create 2")
	data2 := map[string]interface{}{
		"name": "0123456789ABCDEF",
		"age":  9223372036854775807,
	}
	fmt.Println(addToTable("testDB1", variableTableNameWithTime, data2))

	fmt.Println("Create 3")
	data3 := map[string]interface{}{
		"name":  "Hans",
		"age":   40,
		"alter": "alter",
	}
	fmt.Println(addToTable("testDB1", variableTableNameWithTime, data3))

	fmt.Println("Create 4")
	data4 := map[string]interface{}{
		"name":     "Siegfried",
		"city":     "Berlin",
		"longText": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
	}
	fmt.Println(addToTable("testDB1", variableTableNameWithTime, data4))

	fmt.Println("Create 5")
	data5 := map[string]interface{}{
		"age": 50,
	}
	fmt.Println(addToTable("testDB1", variableTableNameWithTime, data5))

	fmt.Println("Create 6")
	data6 := map[string]interface{}{
		"name":       "Peter",
		"age":        30,
		"extraField": "unexpected", // Extra field not in schema
	}
	fmt.Println(addToTable("testDB1", variableTableNameWithTime, data6))

	fmt.Println("Create 7")
	data7 := map[string]interface{}{
		"name": "Peter",
	}
	fmt.Println(addToTable("testDB1", variableTableNameWithTime, data7))

	resp, records := readTable("testDB1", variableTableNameWithTime)
	if resp.num == 200 {
		fmt.Println("Table records:", records)
	} else {
		fmt.Println("Error:", resp.err)
	}

	//lost := 2
	//
	//if lost == 1 {
	//	fmt.Println(createTable("tableData-to_be_deleted", "testDB1", []field{
	//		{"name", "string", 16, []Constraint{"not_null"}},
	//		{"age", "int", 8, []Constraint{}},
	//		{"city", "string", 16, []Constraint{}},
	//	}))
	//} else if lost == 2 {
	//	fmt.Println(deleteTable("testDB1", "tableData-to_be_deleted"))
	//}
}

/*select*/

func printTableDetails(tbl table) {
	fmt.Printf("Name: %s\n", tbl.TableName)
	fmt.Println("Columns:")
	for _, col := range tbl.Fields {
		fmt.Println("-", col)
	}
	fmt.Println("Primary Keys:")
}

/*create*/

func createDatabase(name string) response {

	var pathSchema = mainPath + "/" + name

	if _, err := os.Stat(pathSchema); os.IsNotExist(err) {

		err := os.Mkdir(pathSchema, 0777)
		if err != nil {
			return response{time.Now().String(), 500, fmt.Sprint(err)}
		}

		_, err = os.Create(pathSchema + "/index.conf" + fileEnding)
		if err != nil {
			return response{time.Now().String(), 500, fmt.Sprint(err)}
		}

		return response{time.Now().String(), 200, "All good"}

	} else {
		var errorMessage = "Schema " + name + " already exists"
		return response{time.Now().String(), 406, errorMessage}
	}
}

// Function to create a database table
func createTable(name string, schema string, fields []field) response {
	// Prepend the timePKField to fields
	fields = append([]field{timePKField}, fields...)

	// Set the path for the schema and table
	var pathSchema = mainPath + "/" + schema
	var pathTable = mainPath + "/" + schema + "/" + name + fileEnding
	var pathConf = mainPath + "/" + schema + "/" + name + ".conf" + fileEnding

	// Check schema
	if len(schema) == 0 {
		return response{time.Now().String(), 406, "Have to select a schema"}
	}

	if _, err := os.Stat(pathSchema); os.IsNotExist(err) {
		// Return error if schema does not exist
		var errorMessage = "Schema " + schema + " does not exist"
		return response{time.Now().String(), 406, errorMessage}
	}

	// Check if table exists
	if _, err := os.Stat(pathTable); !os.IsNotExist(err) {
		// Return error if table file already exists
		var errorMessage = "Table " + name + " already exists"
		return response{time.Now().String(), 406, errorMessage}
	}

	// Check table name
	if len(name) == 0 {
		return response{time.Now().String(), 406, "You have to give the table a name"}
	}

	if strings.HasPrefix(name, ".") {
		return response{time.Now().String(), 406, "Can't name a Table like that, sowwy"}
	}

	if name == "index" {
		return response{time.Now().String(), 406, "Can't name a Table \"index\", sowwy"}
	}

	// Validate field lengths
	if err := validateFieldLengths(fields); err != nil {
		return response{time.Now().String(), 406, err.Error()}
	}

	// Create the file for the table
	file, err := os.Create(pathTable)
	defer file.Close() // Close the file after function ends
	if err != nil {
		// Return error if file creation fails
		return response{time.Now().String(), 500, "Failed to create table file: " + err.Error()}
	}

	// Create a separate data file for each ref field
	for _, field := range fields {
		if field.Type == "ref" {
			refFilePath := pathSchema + "/" + name + "." + field.Name + ".data" + fileEnding
			refFile, err := os.Create(refFilePath)
			if err != nil {
				return response{time.Now().String(), 500, "Failed to create ref field file: " + err.Error()}
			}
			refFile.Close()
		}
	}

	confFile, err := os.Create(pathConf)
	if err != nil {
		return response{time.Now().String(), 500, fmt.Sprint(err)}
	}
	defer confFile.Close()

	// Create the configuration file
	newTable := table{
		TableName: name,
		CreatedAt: time.Now().Format(time.RFC3339),
		Fields:    fields,
	}

	// Serialize the table to JSON
	tableJSON, err := json.MarshalIndent(newTable, "", "  ")
	if err != nil {
		return response{time.Now().String(), 500, "Failed to serialize table to JSON: " + err.Error()}
	}

	// Write JSON to configuration file
	err = os.WriteFile(pathConf, tableJSON, 0644)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to write JSON to configuration file: " + err.Error()}
	}

	// Log success message
	return response{time.Now().String(), 200, "Table created successfully"}
}

func validateFieldLengths(fields []field) error {
	for _, f := range fields {
		if f.Type == "ref" && f.Length != Uint128Length {
			return fmt.Errorf("field '%s' of type 'ref' must have a length of %d bytes", f.Name, Uint128Length)
		}
		if f.Type == "timeID" && f.Length != 8 {
			return fmt.Errorf("field '%s' of type 'timeID' must have a length of 8 bytes", f.Name)
		}
	}
	return nil
}

// Updated addToTable function to handle ref fields
func addToTable(schema string, tableName string, data map[string]interface{}) response {
	var pathSchema = mainPath + "/" + schema
	var pathConf = pathSchema + "/" + tableName + ".conf" + fileEnding
	var pathData = pathSchema + "/" + tableName + fileEnding

	// Check if the schema exists
	if _, err := os.Stat(pathSchema); os.IsNotExist(err) {
		return response{time.Now().String(), 406, "Schema " + schema + " does not exist"}
	}

	// Check if the configuration file exists
	if _, err := os.Stat(pathConf); os.IsNotExist(err) {
		return response{time.Now().String(), 406, "Table " + tableName + " does not exist"}
	}

	// Read the table configuration
	configBytes, err := os.ReadFile(pathConf)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to read table configuration: " + err.Error()}
	}

	var tableConfig table
	err = json.Unmarshal(configBytes, &tableConfig)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to parse table configuration: " + err.Error()}
	}

	// Automatically generate the id (timePK) field value
	idValue := generateUniqueTimestamp()
	data["id"] = idValue // Set the id in the data map

	binaryData := []byte{}
	for _, field := range tableConfig.Fields {
		value, exists := data[field.Name]
		if field.Type == "ref" {
			refFilePath := pathSchema + "/" + tableName + "." + field.Name + ".data" + fileEnding
			refFile, err := os.OpenFile(refFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				return response{time.Now().String(), 500, "Failed to open ref field file: " + err.Error()}
			}

			if !exists {
				// Handle missing ref value
				refFile.Close()
				binaryData = append(binaryData, make([]byte, 16)...) // Append zeroed offsets (16 bytes)
				continue
			}

			refValue, ok := value.(string)
			if !ok {
				refFile.Close()
				return response{time.Now().String(), 406, fmt.Sprintf("Field '%s' requires a string value", field.Name)}
			}

			start := int64(0)
			stat, _ := refFile.Stat()
			start = stat.Size()
			_, err = refFile.Write([]byte(refValue + "\n"))
			refFile.Close()

			if err != nil {
				return response{time.Now().String(), 500, "Failed to write to ref field file: " + err.Error()}
			}

			binaryData = append(binaryData, int64ToBytes(start, 8)...)
			binaryData = append(binaryData, int64ToBytes(start+int64(len(refValue)), 8)...)
		} else if exists {
			fieldBytes, err := serializeField(field, value)
			if err != nil {
				return response{time.Now().String(), 406, fmt.Sprintf("Error serializing field '%s': %v", field.Name, err)}
			}
			binaryData = append(binaryData, fieldBytes...)
		} else {
			// Handle missing values for non-ref fields
			if containsConstraint(field.Constraints, NotNull) {
				return response{time.Now().String(), 406, fmt.Sprintf("Field '%s' is required but not provided", field.Name)}
			}
			binaryData = append(binaryData, make([]byte, int(field.Length))...) // Null or default value
		}
	}

	// Write the binary data to the table file
	file, err := os.OpenFile(pathData, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to open table file: " + err.Error()}
	}
	defer file.Close()

	_, err = file.Write(binaryData)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to write data to table file: " + err.Error()}
	}

	return response{time.Now().String(), 200, fmt.Sprintf("Data added to table successfully with id: %d", idValue)}
}

// Utility function to check if a constraint exists in a list
func containsConstraint(constraints []Constraint, target Constraint) bool {
	for _, c := range constraints {
		if c == target {
			return true
		}
	}
	return false
}

// Utility function to serialize a field based on its type and length
func serializeField(field field, value interface{}) ([]byte, error) {
	switch field.Type {
	case "timeID":
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("value must be an int64 for field type 'timeID'")
		}
		return int64ToBytes(v, int(field.Length)), nil
	case "int":
		v, ok := value.(int)
		if !ok {
			return nil, fmt.Errorf("value must be an int for field type 'int'")
		}
		return int64ToBytes(int64(v), int(field.Length)), nil
	case "float":
		v, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("value must be a float64 for field type 'float'")
		}
		return float64ToBytes(v), nil
	case "string":
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value must be a string for field type 'string'")
		}
		return stringToBytes(v, int(field.Length)), nil
	case "ref":
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("value must be a string for field type 'ref'")
		}
		return stringToBytes(v, int(field.Length)), nil
	default:
		return nil, fmt.Errorf("unsupported field type '%s'", field.Type)
	}
}

// Helper functions for type serialization
func int64ToBytes(value int64, length int) []byte {
	bytes := make([]byte, length)
	for i := 0; i < length && i < 8; i++ {
		bytes[i] = byte(value >> (8 * i))
	}
	return bytes
}

func float64ToBytes(value float64) []byte {
	bits := math.Float64bits(value)
	return int64ToBytes(int64(bits), 8)
}

func stringToBytes(value string, length int) []byte {
	bytes := make([]byte, length)
	copy(bytes, value)
	return bytes
}

func readTable(schema string, tableName string) (response, []map[string]interface{}) {
	// Paths
	var pathSchema = mainPath + "/" + schema
	var pathConf = pathSchema + "/" + tableName + ".conf" + fileEnding
	var pathData = pathSchema + "/" + tableName + fileEnding

	// Check if the schema exists
	if _, err := os.Stat(pathSchema); os.IsNotExist(err) {
		return response{time.Now().String(), 406, "Schema " + schema + " does not exist"}, nil
	}

	// Check if the configuration file exists
	if _, err := os.Stat(pathConf); os.IsNotExist(err) {
		return response{time.Now().String(), 406, "Table " + tableName + " does not exist"}, nil
	}

	// Read the table configuration
	configBytes, err := os.ReadFile(pathConf)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to read table configuration: " + err.Error()}, nil
	}

	var tableConfig table
	err = json.Unmarshal(configBytes, &tableConfig)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to parse table configuration: " + err.Error()}, nil
	}

	// Check if the data file exists
	if _, err := os.Stat(pathData); os.IsNotExist(err) {
		return response{time.Now().String(), 406, "No data found for table " + tableName}, nil
	}

	// Read the binary data from the table file
	dataBytes, err := os.ReadFile(pathData)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to read table data: " + err.Error()}, nil
	}

	// Parse the binary data into records
	var records []map[string]interface{}
	recordSize := calculateRecordSize(tableConfig.Fields)
	for i := 0; i < len(dataBytes); i += recordSize {
		if i+recordSize > len(dataBytes) {
			break // Handle partial records gracefully
		}
		recordBytes := dataBytes[i : i+recordSize]
		record := parseRecordWithRefs(recordBytes, tableConfig.Fields, schema, tableName)
		records = append(records, record)
	}

	return response{time.Now().String(), 200, "Table read successfully"}, records
}

// Updated function to parse a binary record, including ref fields
func parseRecordWithRefs(data []byte, fields []field, schema string, tableName string) map[string]interface{} {
	record := make(map[string]interface{})
	offset := 0
	for _, field := range fields {
		fieldData := data[offset : offset+int(field.Length)]
		if field.Type == "timeID" {
			idValue := bytesToInt64(fieldData)
			record[field.Name] = idValue
			record["id_datetime"] = time.Unix(0, idValue).Format(time.RFC3339Nano) // Convert id to datetime
		} else if field.Type == "ref" {
			// Extract start and end offsets
			start := bytesToInt64(fieldData[:8])
			end := bytesToInt64(fieldData[8:])
			refFilePath := fmt.Sprintf("%s/%s.%s.data%s", mainPath+"/"+schema, tableName, field.Name, fileEnding)

			// Read the referenced data from the ref file
			refData, err := os.ReadFile(refFilePath)
			if err != nil {
				record[field.Name] = fmt.Sprintf("Error reading ref data: %v", err)
			} else if start < int64(len(refData)) && end <= int64(len(refData)) {
				record[field.Name] = string(refData[start:end]) // Extract the referenced string
			} else {
				record[field.Name] = "Invalid reference range"
			}
		} else {
			record[field.Name] = deserializeField(field, fieldData)
		}
		offset += int(field.Length)
	}
	return record
}

// Updated function to parse a binary record into a map with datetime for id
func parseRecordWithDate(data []byte, fields []field) map[string]interface{} {
	record := make(map[string]interface{})
	offset := 0
	for _, field := range fields {
		fieldData := data[offset : offset+int(field.Length)]
		if field.Type == "timeID" {
			idValue := bytesToInt64(fieldData)
			record[field.Name] = idValue
			record["id_datetime"] = time.Unix(0, idValue).Format(time.RFC3339Nano) // Convert id to datetime
		} else {
			record[field.Name] = deserializeField(field, fieldData)
		}
		offset += int(field.Length)
	}
	return record
}

// Utility functions for deserialization
func deserializeField(field field, data []byte) interface{} {
	switch field.Type {
	case "int":
		return bytesToInt64(data)
	case "float":
		return bytesToFloat64(data)
	case "string", "ref":
		return strings.TrimRight(string(data), "\x00") // Trim null bytes
	default:
		return nil
	}
}

// Helper functions for type deserialization
func bytesToInt64(data []byte) int64 {
	value := int64(0)
	for i := 0; i < len(data); i++ {
		value |= int64(data[i]) << (8 * i)
	}
	return value
}

func bytesToFloat64(data []byte) float64 {
	bits := bytesToInt64(data)
	return math.Float64frombits(uint64(bits))
}

// Utility function to calculate the size of a record in bytes
func calculateRecordSize(fields []field) int {
	size := 0
	for _, field := range fields {
		size += int(field.Length)
	}
	return size
}

func generateUniqueTimestamp() int64 {
	for {
		newTimestamp := time.Now().UnixNano()
		current := atomic.LoadInt64(&lastTimestamp)

		// If the new timestamp is greater, attempt to update
		if newTimestamp > current {
			if atomic.CompareAndSwapInt64(&lastTimestamp, current, newTimestamp) {
				return newTimestamp
			}
		} else {
			// Increment the current timestamp to ensure uniqueness
			if atomic.CompareAndSwapInt64(&lastTimestamp, current, current+1) {
				return current + 1
			}
		}
	}
}

func deleteTable(schema string, tableName string) response {
	// Paths
	var pathSchema = mainPath + "/" + schema
	var pathConf = pathSchema + "/" + tableName + ".conf" + fileEnding
	var pathData = pathSchema + "/" + tableName + fileEnding

	// Check if the schema exists
	if _, err := os.Stat(pathSchema); os.IsNotExist(err) {
		return response{time.Now().String(), 406, "Schema " + schema + " does not exist"}
	}

	// Check if the configuration file exists
	if _, err := os.Stat(pathConf); os.IsNotExist(err) {
		return response{time.Now().String(), 406, "Table " + tableName + " does not exist"}
	}

	// Remove the configuration file
	err := os.Remove(pathConf)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to delete table configuration: " + err.Error()}
	}

	// Remove the table file
	err = os.Remove(pathData)
	if err != nil {
		return response{time.Now().String(), 500, "Failed to delete table data: " + err.Error()}
	}

	return response{time.Now().String(), 200, "Table deleted successfully"}
}
