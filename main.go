package main

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	
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

	root_db_username, root_db_username_err := getUsername("ROOT")
	if root_db_username_err != nil {
		errors = append(errors, root_db_username_err...)
	}
	
	migration_db_username, migration_db_username_err := getUsername("MIGRATION")
	if migration_db_username_err != nil {
		errors = append(errors, migration_db_username_err...)
	}

	write_db_username, write_db_username_err := getUsername("WRITE")
	if write_db_username_err != nil {
		errors = append(errors, write_db_username_err...)
	}


	read_db_username, read_db_username_err := getUsername("READ")
	if read_db_username_err != nil {
		errors = append(errors, read_db_username_err...)
	}

	db_hostname, db_hostname_errs := getDatabaseHostname()
	if db_hostname_errs != nil {
		errors = append(errors, db_hostname_errs...)
	}
	
	db_port_number, port_number_errs := getPortNumber()
	if port_number_errs != nil {
		errors = append(errors, port_number_errs...)
	}	

	db_name, db_name_errs := getDatabaseName()
	if db_name_errs != nil {
		errors = append(errors, db_name_errs...)
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

func getDatabaseName() (string, []error) {
	var errors []error
	environment_variable := "HOLISTIC_DB_NAME"
	database_name := os.Getenv(environment_variable)
	if database_name == "" {
		errors = append(errors, fmt.Errorf("%s environment variable not set", environment_variable))
		return "", errors
	}

	database_name_errors := validateDatabaseName(database_name)
	if database_name_errors != nil {
		return "", database_name_errors
	}

	return database_name, nil
}

func validateDatabaseName(db_name string) []error {
	var errors []error
	db_name_regex_name_exp := `^[A-Za-z]+$`
	db_name_regex_name_matcher, db_name_regex_name_matcher_errors := regexp.Compile(db_name_regex_name_exp)
	if db_name_regex_name_matcher_errors != nil {
		errors = append(errors, fmt.Errorf("database name regex %s did not compile %s", db_name_regex_name_exp, db_name_regex_name_matcher_errors.Error()))
		return errors
	}

	if !db_name_regex_name_matcher.MatchString(db_name) {
		errors = append(errors, fmt.Errorf("database name %s did not match regex %s", db_name, db_name_regex_name_exp))
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

func validateUsername(username string) []error {
	var errors []error
	regex_name_exp := `^[A-Za-z]+$`
	regex_name_matcher, regex_name_matcher_errors := regexp.Compile(regex_name_exp)
	if regex_name_matcher_errors != nil {
		errors = append(errors, fmt.Errorf("username regex %s did not compile %s", regex_name_exp, regex_name_matcher_errors.Error()))
		return errors
	}

	if !regex_name_matcher.MatchString(username) {
		errors = append(errors, fmt.Errorf("username %s did not match regex %s", username, regex_name_exp))
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

func getPortNumber() (string, []error) {
	var errors []error
	environment_variable := "HOLISTIC_DB_PORT_NUMBER"
	port_number := os.Getenv(environment_variable)
	if port_number == "" {
		errors = append(errors, fmt.Errorf("%s environment variable not set", environment_variable))
		return "", errors
	}

	port_number_errors := validatePortNumber(port_number)
	if port_number_errors != nil {
		return "", port_number_errors
	}

	return port_number, nil
}

func validatePortNumber(db_port_number string) []error {
	var errors []error
	portnumber_regex_name_exp := `\d+`
	portnumber_regex_name_matcher, port_number_regex_name_matcher_errors := regexp.Compile(portnumber_regex_name_exp)
	if port_number_regex_name_matcher_errors != nil {
		errors = append(errors, fmt.Errorf("portnumber regex %s did not compile %s", portnumber_regex_name_exp, port_number_regex_name_matcher_errors.Error()))
		return errors
	}

	if !portnumber_regex_name_matcher.MatchString(db_port_number) {
		errors = append(errors, fmt.Errorf("portnumber %s did not match regex %s", db_port_number, portnumber_regex_name_exp))
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

func getDatabaseHostname() (string, []error) {
	var errors []error
	environment_variable := "HOLISTIC_DB_HOSTNAME"
	host_name := os.Getenv(environment_variable)
	if host_name == "" {
		errors = append(errors, fmt.Errorf("%s environment variable not set", environment_variable))
		return "", errors
	}

	host_name_errors := validateHostname(host_name)
	if host_name_errors != nil {
		return "", host_name_errors
	}

	return host_name, nil
}

func validateHostname(db_hostname string) []error {
	var errors []error

	simpleHostname := false
	ipAddress := true
	complexHostname := true

	hostname_regex_name_exp := `^[A-Za-z]+$`
	hostname_regex_name_matcher, hostname_regex_name_matcher_errors := regexp.Compile(hostname_regex_name_exp)
	if hostname_regex_name_matcher_errors != nil {
		errors = append(errors, fmt.Errorf("username regex %s did not compile %s", hostname_regex_name_exp, hostname_regex_name_matcher_errors.Error()))
	}

	simpleHostname = hostname_regex_name_matcher.MatchString(db_hostname)

	parts := strings.Split(db_hostname, ".")
	if len(parts) == 4 {
		for _, value := range parts {
			_, err := strconv.Atoi(value)
			if err != nil {
				ipAddress = false
			}
		}
	}

	for _, value := range parts {
		if !hostname_regex_name_matcher.MatchString(value) {
			complexHostname = false
		}
	}

	if !(simpleHostname || complexHostname || ipAddress) {
		errors = append(errors, fmt.Errorf("hostname name is invalid %s", db_hostname))
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

func getUsername(label string) (string, []error) {
	var errors []error
	environment_variable := "HOLISTIC_DB_" + label + "_USERNAME"
	username := os.Getenv(environment_variable)
	if username == "" {
		errors = append(errors, fmt.Errorf("%s environment variable not set", environment_variable))
		return "", errors
	}

	username_errors := validateUsername(username)
	if username_errors != nil {
		return "", username_errors
	}

	return username, nil
}
