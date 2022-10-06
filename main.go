package main

import (
	"fmt"
	"os"
	"io/ioutil"
	"strings"
	"bufio"
	class "github.com/matehaxor03/holistic_db_client/class"
)

func main() {
	errors := InitDB()
	if errors != nil {
		fmt.Println(fmt.Errorf("%s", errors))
		os.Exit(1)
	}

	os.Exit(0)
}

func InitDB() []error {
	var errors []error

	db_hostname, db_port_number, _, root_db_username, _, root_details_errors := getDetails("root")
	if root_details_errors != nil {
		errors = append(errors, root_details_errors...)
	}
	
	_, _, db_name, migration_db_username, migration_db_password, migration_details_errors := getDetails("migration")
	if migration_details_errors != nil {
		errors = append(errors, migration_details_errors...)
	}

	_, _, _, write_db_username, write_db_password, write_details_errors := getDetails("write")
	if write_details_errors != nil {
		errors = append(errors, write_details_errors...)
	}

	_, _, _, read_db_username, read_db_password, read_details_errors := getDetails("read")
	if read_details_errors != nil {
		errors = append(errors, read_details_errors...)
	}

	if len(errors) > 0 {
		return errors
	}

	usernames := [...]string{root_db_username, migration_db_username, write_db_username, read_db_username}

	usernamesGrouped := make(map[string]int)
	for _, num := range usernames {
		usernamesGrouped[num] = usernamesGrouped[num] + 1
	}

	for key, element := range usernamesGrouped {
		if element > 1 {
			errors = append(errors, fmt.Errorf("%s database username was detected %d times - root, migration, write and read database usernames must be all unqiue", key, element))
		}
	}

	if len(errors) > 0 {
		return errors
	}

	host, host_errors := class.NewHost(&db_hostname, &db_port_number)
	client, client_errors := class.NewClient(host, &root_db_username, nil)

	encoding := "utf8"
	collate := "utf8_general_ci"
	database_create_options := class.NewDatabaseCreateOptions(&encoding, &collate)
	
	options := make(map[string]map[string][][]string)
	logic_options := make(map[string][][]string)
	logic_options["CREATE"] = append(logic_options["CREATE"], class.GET_LOGIC_STATEMENT_IF_NOT_EXISTS())
	options[class.GET_LOGIC_STATEMENT_FIELD_NAME()] = logic_options

	if host_errors != nil {
		errors = append(errors, host_errors...)
	}

	if client_errors != nil {
		errors = append(errors, client_errors...)
	}

	if len(errors) > 0 {
		return errors
	}
	
	database, result, database_creation_errs := client.CreateDatabase(&db_name, database_create_options, options)
	if database_creation_errs != nil {
		errors = append(errors, database_creation_errs...)
		fmt.Println(fmt.Errorf("%s", *result))
		return errors
	}
	
	use_database_errors := client.UseDatabase(database)
	if use_database_errors != nil {
		return use_database_errors
	}

	localhost_IP := "127.0.0.1"
	migration_db_user, _, create_migration_user_errs := client.CreateUser(&migration_db_username, &migration_db_password, &localhost_IP, options)
	if create_migration_user_errs != nil {
		return create_migration_user_errs
	}

	_, _, grant_migration_db_user_errors := client.Grant(migration_db_user, "ALL", "*")
	if grant_migration_db_user_errors != nil {
		return grant_migration_db_user_errors
	}

	write_db_user, _, write_user_errs := client.CreateUser(&write_db_username, &write_db_password, &localhost_IP, options)
	if write_user_errs != nil {
		return write_user_errs
	}

	_, _, grant_write_db_user_errors := client.Grant(write_db_user, "INSERT", "*")
	if grant_write_db_user_errors != nil {
		return grant_write_db_user_errors
	}

	_, _, grant_write_db_user_errors2 := client.Grant(write_db_user, "UPDATE", "*")
	if grant_write_db_user_errors2 != nil {
		return grant_write_db_user_errors2
	}

	read_db_user, _, read_user_errs := client.CreateUser(&read_db_username, &read_db_password, &localhost_IP, options)
	if read_user_errs != nil {
		return read_user_errs
	}

	_, _, grant_read_db_user_errors := client.Grant(read_db_user, "SELECT", "*")
	if grant_read_db_user_errors != nil {
		return grant_read_db_user_errors
	}

	/*
	_, create_table_database_migration_err := db.Exec("CREATE TABLE IF NOT EXISTS DatabaseMigration (databaseMigrationId BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, current BIGINT NOT NULL DEFAULT -1, desired BIGINT NOT NULL DEFAULT 0, created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, last_modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	if create_table_database_migration_err != nil {
		fmt.Println("error creating database_migration table")
		errors = append(errors, create_table_database_migration_err)
		return errors
	}

	db_results, count_err := db.Query("SELECT COUNT(*) FROM DatabaseMigration")
	if count_err != nil {
		fmt.Println("error fetching count of records for DatabaseMigration")
		errors = append(errors, count_err)
		return errors
	}

	defer db_results.Close()
	var count int

	for db_results.Next() {
		if err := db_results.Scan(&count); err != nil {
			errors = append(errors, err)
			return errors
		}
	}

	if count > 0 {
		return nil
	}

	_, insert_record_database_migration_err := db.Exec("INSERT INTO DatabaseMigration () VALUES ()")
	if insert_record_database_migration_err != nil {
		fmt.Println("error inserting record into database_migration")
		errors = append(errors, insert_record_database_migration_err)
		return errors
	}
	*/
	return nil
}

func getDetails(username string) (string, string, string, string, string, []error) {
	var errors []error

	files, err := ioutil.ReadDir("./")
    if err != nil {
		errors = append(errors, err)
		return "", "", "", "", "", errors
    }

	filename := ""
    for _, file := range files {
		if file.IsDir() {
			continue
		}

		currentFileName := file.Name()

		if !strings.HasPrefix(currentFileName, "holistic_db_config:") {
			continue
		}

		if !strings.HasSuffix(currentFileName, username + ".config") {
			continue
		}		
		filename = currentFileName
    }

	if filename == "" {
		errors = append(errors, fmt.Errorf("database config for %s not found ust be in the format: holistic_db_config|{database_ip_address}|{database_port_number}|{database_name}|{database_username}.config e.g holistic_db_config|127.0.0.1|3306|holistic|root.config", username))
		return "", "", "", "", "", errors
	}

	parts := strings.Split(filename, ":")
	if len(parts) != 5 {
		errors = append(errors, fmt.Errorf("database config for %s not found ust be in the format: holistic_db_config|{database_ip_address}|{database_port_number}|{database_name}|{database_username}.config e.g holistic_db_config|127.0.0.1|3306|holistic|root.config", username))
		return "", "", "", "", "", errors
	}

	ip_address := parts[1]
	port_number := parts[2]
	database_name := parts[3]

	password := ""
	
	file, err_file := os.Open(filename)

    if err_file != nil {
        errors = append(errors, err_file)
		return "", "", "", "", "", errors
    }

    defer file.Close()

    scanner := bufio.NewScanner(file)

    for scanner.Scan() {
		currentText := scanner.Text()
		if strings.HasPrefix(currentText, "password=") {
			password = currentText[9:len(currentText)]
		}
    }

    if file_errs := scanner.Err(); err != nil {
        errors = append(errors, file_errs)
    }

	if password == "" {
		errors = append(errors, fmt.Errorf("password not found for file: %s", filename))
	}

	if len(errors) > 0 {
		return "", "", "", "", "", errors
	}
	
	return ip_address, port_number, database_name, username, password, errors
}
