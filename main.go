package main

import (
	"fmt"
	"os"
	"io/ioutil"
	"strings"
	"bufio"
	"strconv"
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

	db_hostname, db_port_number, _, root_db_username, root_db_password, root_details_errors := getDetails("root")
	if root_details_errors != nil {
		errors = append(errors, root_details_errors...)
	}
	
	_, _, db_name, migration_db_username, migration_db_password, migration_details_errors := getDetails("holistic_migration")
	if migration_details_errors != nil {
		errors = append(errors, migration_details_errors...)
	}

	_, _, _, write_db_username, write_db_password, write_details_errors := getDetails("holistic_write")
	if write_details_errors != nil {
		errors = append(errors, write_details_errors...)
	}

	_, _, _, read_db_username, read_db_password, read_details_errors := getDetails("holistic_read")
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
			errors = append(errors, fmt.Errorf("database username: %s was detected %d times - root, holistic_migration, holistic_write and holistic_read database usernames must be all unqiue", key, element))
		}
	}

	passwords := [...]string{root_db_password, migration_db_password, write_db_password, read_db_password}

	passwordsGrouped := make(map[string]int)
	for _, num := range passwords {
		passwordsGrouped[num] = passwordsGrouped[num] + 1
	}

	for _, element := range passwordsGrouped {
		if element > 1 {
			errors = append(errors, fmt.Errorf("database password was detected %d times - root, holistic_migration, holistic_write and holistic_read database passwords must be all unqiue", element))
		}
	}

	if len(errors) > 0 {
		return errors
	}

	host, host_errors := class.NewHost(&db_hostname, &db_port_number)
	client, client_errors := class.NewClient(host, &root_db_username, nil)

	encoding := "utf8mb4"
	collate := "utf8mb4_0900_ai_ci"
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
	
	fmt.Println("creating database...")
	database, _, database_creation_errs := client.CreateDatabase(&db_name, database_create_options, options)
	if database_creation_errs != nil {
		errors = append(errors, database_creation_errs...)		
		return errors
	}

	use_database_errors := client.UseDatabase(database)
	if use_database_errors != nil {
		return use_database_errors
	}

	fmt.Println("creating migration database user...")
	migration_db_user, _, create_migration_user_errs := client.CreateUser(&migration_db_username, &migration_db_password, &db_hostname, options)
	if create_migration_user_errs != nil {
		return create_migration_user_errs
	} else {
		fmt.Println("updating migration database user password...")
		update_password_errs := migration_db_user.UpdatePassword(migration_db_password)
		if update_password_errs != nil {
			return update_password_errs
		}
	}

	fmt.Println("granting permissions to migration database user...")
	_, _, grant_migration_db_user_errors := client.Grant(migration_db_user, "ALL", "*")
	if grant_migration_db_user_errors != nil {
		return grant_migration_db_user_errors
	}

	fmt.Println("creating write database user...")
	write_db_user, _, write_user_errs := client.CreateUser(&write_db_username, &write_db_password, &db_hostname, options)
	if write_user_errs != nil {
		return write_user_errs
	} else {
		fmt.Println("updating write database user password...")
		update_password_errs := write_db_user.UpdatePassword(write_db_password)
		if update_password_errs != nil {
			return update_password_errs
		}
	}

	fmt.Println("granting permissions to write database user...")
	_, _, grant_write_db_user_errors := client.Grant(write_db_user, "INSERT", "*")
	if grant_write_db_user_errors != nil {
		return grant_write_db_user_errors
	}

	_, _, grant_write_db_user_errors2 := client.Grant(write_db_user, "UPDATE", "*")
	if grant_write_db_user_errors2 != nil {
		return grant_write_db_user_errors2
	}

	fmt.Println("creating read database user...")
	read_db_user, _, read_user_errs := client.CreateUser(&read_db_username, &read_db_password, &db_hostname, options)
	if read_user_errs != nil {
		return read_user_errs
	} else {
		fmt.Println("updating read database user password...")
		update_password_errs := read_db_user.UpdatePassword(read_db_password)
		if update_password_errs != nil {
			return update_password_errs
		}
	}

	fmt.Println("granting permissions to read database user...")
	_, _, grant_read_db_user_errors := client.Grant(read_db_user, "SELECT", "*")
	if grant_read_db_user_errors != nil {
		return grant_read_db_user_errors
	}

	use_database_username_errors := client.UseDatabaseUsername(&migration_db_username)
	if use_database_username_errors != nil {
		return use_database_username_errors
	}



	database_migration_schema := class.Map {
		"[table_name]": class.Map {"type": "*string", "value": "DatabaseMigration"},
		"database_migration_id": class.Map {"type": "*int64", "unsigned": true, "auto_increment": true, "primary_key": true},
		"current": class.Map {"type": "*int64", "default": -1},
		"desired": class.Map {"type": "*int64", "default": 0},
	}
	

	fmt.Println("creating table database migration...")
	data_migration_table, _, create_table_errors := database.CreateTable(database_migration_schema, options)
	if create_table_errors != nil {
		return create_table_errors
	}

	data_migration_table_record_count, data_migration_table_record_count_errors := data_migration_table.Count()
	if data_migration_table_record_count_errors != nil {
		return data_migration_table_record_count_errors
	}


	if *data_migration_table_record_count > 0 {
		return nil
	}

	fmt.Println("creating database migration table record...")
	inserted_record, inserted_record_errors := data_migration_table.CreateRecord(class.Map{})
	if inserted_record_errors != nil {
		return inserted_record_errors
	}
	fmt.Println(fmt.Sprintf("created recrod with primary key: %s", strconv.FormatUint(*(inserted_record.GetUInt64("database_migration_id")), 10)))

	return nil
}

func getDetails(label string) (string, string, string, string, string, []error) {
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

		if !strings.HasSuffix(currentFileName, label + ".config") {
			continue
		}		
		filename = currentFileName
    }

	if filename == "" {
		errors = append(errors, fmt.Errorf("database config for %s not found ust be in the format: holistic_db_config|{database_ip_address}|{database_port_number}|{database_name}|{database_username}.config e.g holistic_db_config|127.0.0.1|3306|holistic|root.config", label))
		return "", "", "", "", "", errors
	}

	parts := strings.Split(filename, ":")
	if len(parts) != 5 {
		errors = append(errors, fmt.Errorf("database config for %s not found ust be in the format: holistic_db_config|{database_ip_address}|{database_port_number}|{database_name}|{database_username}.config e.g holistic_db_config|127.0.0.1|3306|holistic|root.config", label))
		return "", "", "", "", "", errors
	}

	ip_address := parts[1]
	port_number := parts[2]
	database_name := parts[3]

	password := ""
	username := ""
	
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

		if strings.HasPrefix(currentText, "user=") {
			username = currentText[5:len(currentText)]
		}
    }

    if file_errs := scanner.Err(); err != nil {
        errors = append(errors, file_errs)
    }

	if password == "" {
		errors = append(errors, fmt.Errorf("password not found for file: %s", filename))
	}

	if username == "" {
		errors = append(errors, fmt.Errorf("user not found for file: %s", filename))
	}

	if len(errors) > 0 {
		return "", "", "", "", "", errors
	}

	return ip_address, port_number, database_name, username, password, errors
}
