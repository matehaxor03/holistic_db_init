package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"bufio"
	"io/ioutil"
	json "github.com/matehaxor03/holistic_json/json"
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

	db_hostname, db_port_number, _, root_db_username, root_db_password, root_details_errors := getCredentialDetails("root")
	if root_details_errors != nil {
		errors = append(errors, root_details_errors...)
	}
	
	_, _, db_name, migration_db_username, migration_db_password, migration_details_errors := getCredentialDetails("holistic_migration")
	if migration_details_errors != nil {
		errors = append(errors, migration_details_errors...)
	}

	_, _, _, write_db_username, write_db_password, write_details_errors := getCredentialDetails("holistic_write")
	if write_details_errors != nil {
		errors = append(errors, write_details_errors...)
	}

	_, _, _, read_db_username, read_db_password, read_details_errors := getCredentialDetails("holistic_read")
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

	client_manager, client_manager_errors := class.NewClientManager()
	if client_manager_errors != nil {
		errors = append(errors, client_manager_errors...)
	}

	if len(errors) > 0 {
		return errors
	}

	client, client_errors := client_manager.GetClient("holistic_db_config:" + db_hostname + ":" + db_port_number + ":" + db_name + ":" + root_db_username)
	if client_errors != nil {
		errors = append(errors, client_errors...)
	}

	if len(errors) > 0 {
		return errors
	}

	disable_global_logs_errors := client.GlobalGeneralLogDisable()
	if disable_global_logs_errors != nil {
		return disable_global_logs_errors
	}

	set_utc_time_errors := client.GlobalSetTimeZoneUTC()
	if set_utc_time_errors != nil {
		return set_utc_time_errors
	}

	set_sql_mode_errors := client.GlobalSetSQLMode()
	if set_sql_mode_errors != nil {
		return set_sql_mode_errors
	}

	database_exists, database_exists_errors := client.DatabaseExists(db_name)
	if database_exists_errors != nil {
		return database_exists_errors
	}
	
	if !(*database_exists) {
		character_set := class.GET_CHARACTER_SET_UTF8MB4()
		collate := class.GET_COLLATE_UTF8MB4_0900_AI_CI()

		fmt.Println("creating database...")
		_, database_creation_errs := client.CreateDatabase(db_name, &character_set, &collate)
		if database_creation_errs != nil {
			errors = append(errors, database_creation_errs...)		
			return errors
		}
	} else {
		fmt.Println("(skip) database already exists...")
	}

	database, database_errors := client.GetDatabase()
	if database_errors != nil {
		fmt.Println("get database errors ...")
		return database_errors
	}

	use_database_errors := client.UseDatabase(*database)
	if use_database_errors != nil {
		fmt.Println("use database errors ...")
		return use_database_errors
	}

	database_filter := db_name
	table_filter := "*"

	migration_user_exists, migration_user_exists_errors := client.UserExists(migration_db_username)
	if migration_user_exists_errors != nil {
		fmt.Println("migration user exists errors ...")
		return migration_user_exists_errors
	}

	if !(*migration_user_exists) {
		fmt.Println("creating migration database user...")
		migration_db_user, create_migration_user_errs := client.CreateUser(migration_db_username, migration_db_password, db_hostname)
		if create_migration_user_errs != nil {
			return create_migration_user_errs
		} else {
			fmt.Println("updating migration database user password...")
			update_password_errs := migration_db_user.UpdatePassword(migration_db_password)
			if update_password_errs != nil {
				return update_password_errs
			}
		}
	} else {
		fmt.Println("(skip) migration database user already exists...")
	}
	
	migration_db_user, migration_db_user_errors := client.GetUser(migration_db_username)
	if migration_db_user_errors != nil {
		fmt.Println("get migration user exists errors ...")
		return migration_db_user_errors
	}

	fmt.Println("granting permissions to migration database user...")
	_, grant_migration_db_user_errors := client.Grant(*migration_db_user, "ALL", &database_filter, &table_filter)
	if grant_migration_db_user_errors != nil {
		return grant_migration_db_user_errors
	}

	write_user_exists, write_user_exists_errors := client.UserExists(write_db_username)
	if write_user_exists_errors != nil {
		return write_user_exists_errors
	}
	if !(*write_user_exists) {
		fmt.Println("creating write database user...")
		write_db_user, create_write_user_errs := client.CreateUser(write_db_username, write_db_password, db_hostname)
		if create_write_user_errs != nil {
			return create_write_user_errs
		} else {
			fmt.Println("updating write database user password...")
			update_password_errs := write_db_user.UpdatePassword(write_db_password)
			if update_password_errs != nil {
				return update_password_errs
			}
		}
	} else {
		fmt.Println("(skip) write database user already exists...")
	}
	write_db_user, write_db_user_errors := client.GetUser(write_db_username)
	if write_db_user_errors != nil {
		return write_db_user_errors
	}

	fmt.Println("granting permissions to write database user...")
	_, grant_write_db_user_errors := client.Grant(*write_db_user, "INSERT", &database_filter, &table_filter)
	if grant_write_db_user_errors != nil {
		return grant_write_db_user_errors
	}

	_, grant_write_db_user_errors2 := client.Grant(*write_db_user, "UPDATE", &database_filter, &table_filter)
	if grant_write_db_user_errors2 != nil {
		return grant_write_db_user_errors2
	}

	_, grant_write_db_user_errors3 := client.Grant(*write_db_user, "SELECT", &database_filter, &table_filter)
	if grant_write_db_user_errors3 != nil {
		return grant_write_db_user_errors3
	}


	read_user_exists, read_user_exists_errors := client.UserExists(read_db_username)
	if read_user_exists_errors != nil {
		return read_user_exists_errors
	}
	if !(*read_user_exists) {
		fmt.Println("creating read database user...")
		read_db_user, create_read_user_errs := client.CreateUser(read_db_username, read_db_password, db_hostname)
		if create_read_user_errs != nil {
			return create_read_user_errs
		} else {
			fmt.Println("updating read database user password...")
			update_password_errs := read_db_user.UpdatePassword(read_db_password)
			if update_password_errs != nil {
				return update_password_errs
			}
		}
	} else {
		fmt.Println("(skip) read database user already exists...")
	}
	read_db_user, read_db_user_errors := client.GetUser(read_db_username)
	if read_db_user_errors != nil {
		return read_db_user_errors
	}

	fmt.Println("granting permissions to read database user...")
	_, grant_read_db_user_errors := client.Grant(*read_db_user, "SELECT", &database_filter, &table_filter)
	if grant_read_db_user_errors != nil {
		return grant_read_db_user_errors
	}

	use_database_username_errors := client.UseDatabaseUsername(migration_db_username)
	if use_database_username_errors != nil {
		return use_database_username_errors
	}
	

	data_migration_table_exists, data_migration_table_exists_errors := database.TableExists("DatabaseMigration")
	if data_migration_table_exists_errors != nil {
		return data_migration_table_exists_errors
	}

	if !(*data_migration_table_exists) {

		database_migration_schema := json.Map {"database_migration_id": json.Map {"type": "uint64", "auto_increment": true, "primary_key": true},
			"current": json.Map {"type": "int64", "default": int64(-1)},
			"desired": json.Map {"type": "int64", "default": int64(0)},
		}

		fmt.Println("creating table database migration...")
		_, create_table_errors := database.CreateTable("DatabaseMigration", database_migration_schema)
		if create_table_errors != nil {
			return create_table_errors
		}
	} else {
		fmt.Println("(skip) table database migration already exists...")
	}

	data_migration_table, data_migration_table_errors := database.GetTable("DatabaseMigration")

	if data_migration_table_errors != nil {
		return data_migration_table_errors
	}
	
	data_migration_table_record_count, data_migration_table_record_count_errors := data_migration_table.Count()
	if data_migration_table_record_count_errors != nil {
		return data_migration_table_record_count_errors
	}

	if *data_migration_table_record_count > 0 {
		fmt.Println("(skip) database migration record already exists...")
		return nil
	}

	fmt.Println("creating database migration record...")
	inserted_record, inserted_record_errors := data_migration_table.CreateRecord(json.Map{"name":"config"})
	if inserted_record_errors != nil {
		return inserted_record_errors
	}

	inserted_record_value, inserted_record_value_errors := inserted_record.GetUInt64("database_migration_id")
	if inserted_record_value_errors != nil {
		return inserted_record_value_errors
	}

	fmt.Println(fmt.Sprintf("created database migration record with primary key: %s", strconv.FormatUint(*inserted_record_value, 10)))
	return nil
}


func getCredentialDetails(label string) (string, string, string, string, string, []error) {
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

		if !strings.HasSuffix(currentFileName, label+".config") {
			continue
		}
		filename = currentFileName
	}

	if filename == "" {
		errors = append(errors, fmt.Errorf("database config for %s not found filename is empty: holistic_db_config|{database_ip_address}|{database_port_number}|{database_name}|{database_username}.config e.g holistic_db_config|127.0.0.1|3306|holistic|root.config", label))
		return "", "", "", "", "", errors
	}

	parts := strings.Split(filename, ":")
	if len(parts) != 5 {
		errors = append(errors, fmt.Errorf("database config for %s not found filename is in wrong format and had parts: %s holistic_db_config|{database_ip_address}|{database_port_number}|{database_name}|{database_username}.config e.g holistic_db_config|127.0.0.1|3306|holistic|root.config", label, parts))
		return "", "", "", "", "", errors
	}

	ip_address := parts[1]
	port_number := parts[2]
	database_name := parts[3]
	username := parts[4]
	password := ""
	username = ""

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