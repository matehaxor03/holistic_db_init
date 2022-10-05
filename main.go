package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	
	class "github.com/matehaxor03/holistic_db_client/class"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
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

	root_db_username, root_db_password := getCredentials("ROOT")
	root_db_credentials_errs := validateCredentials(root_db_username, root_db_password)

	if root_db_credentials_errs != nil {
		errors = append(errors, root_db_credentials_errs...)
	}

	migration_db_username, migration_db_password := getCredentials("MIGRATION")
	migration_db_credentials_errs := validateCredentials(migration_db_username, migration_db_password)

	if migration_db_credentials_errs != nil {
		errors = append(errors, migration_db_credentials_errs...)
	}

	write_db_username, write_db_password := getCredentials("WRITE")
	write_db_credentials_errs := validateCredentials(write_db_username, write_db_password)

	if write_db_credentials_errs != nil {
		errors = append(errors, write_db_credentials_errs...)
	}

	read_db_username, read_db_password := getCredentials("READ")
	read_db_credentials_errs := validateCredentials(read_db_username, read_db_password)

	if read_db_credentials_errs != nil {
		errors = append(errors, read_db_credentials_errs...)
	}

	db_hostname := getDatabaseHostname()
	db_hostname_errors := validateHostname(db_hostname)
	if db_hostname_errors != nil {
		errors = append(errors, db_hostname_errors...)
	}

	db_port_number := getPortNumber()
	db_port_number_err := validatePortNumber(db_port_number)
	if db_port_number_err != nil {
		errors = append(errors, db_port_number_err...)
	}

	db_name := getDatabaseName()
	db_name_err := validateDatabaseName(db_name)
	if db_name_err != nil {
		errors = append(errors, db_name_err...)
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

	root_db_password = base64.StdEncoding.EncodeToString([]byte(root_db_password))
	migration_db_password = base64.StdEncoding.EncodeToString([]byte(migration_db_password))
	write_db_password = base64.StdEncoding.EncodeToString([]byte(migration_db_password))
	read_db_password = base64.StdEncoding.EncodeToString([]byte(migration_db_password))

	if len(errors) > 0 {
		return errors
	}

	cfg_root := mysql.Config{
		User:   root_db_username,
		Passwd: root_db_password,
		Net:    "tcp",
		Addr:   db_hostname + ":" + db_port_number,
	}

	db, dberr := sql.Open("mysql", cfg_root.FormatDSN())

	if dberr != nil {
		errors = append(errors, dberr)
		defer db.Close()
		return errors
	}

	host, host_errors := class.NewHost(&db_hostname, &db_port_number)
	credentials, credential_errors :=  class.NewCredentials(&root_db_username, &root_db_password)
	client, client_errors := class.NewClient(host, credentials, nil)

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

	if credential_errors != nil {
		errors = append(errors, credential_errors...)
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
		errors = append(errors, create_migration_user_errs...)
		return errors
	}

	_, _, grant_migration_db_user_errors := client.Grant(migration_db_user, "ALL", "*")
	if grant_migration_db_user_errors != nil {
		return grant_migration_db_user_errors
	}

	write_db_user, _, write_user_errs := client.CreateUser(&write_db_username, &write_db_password, &localhost_IP, options)
	if write_user_errs != nil {
		errors = append(errors, write_user_errs...)
		return errors
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
		errors = append(errors, read_user_errs...)
		return errors
	}

	_, _, grant_read_db_user_errors := client.Grant(read_db_user, "SELECT", "*")
	if grant_read_db_user_errors != nil {
		return grant_read_db_user_errors
	}

	db.Close()

	cfg_migration := mysql.Config{
		User:   migration_db_username,
		Passwd: migration_db_password,
		Net:    "tcp",
		Addr:   db_hostname + ":" + db_port_number,
		DBName: db_name,
	}

	db, dberr = sql.Open("mysql", cfg_migration.FormatDSN())

	if dberr != nil {
		errors = append(errors, dberr)
		return errors
	}

	defer db.Close()


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

	return nil
}

func getDatabaseName() string {
	return os.Getenv("HOLISTIC_DB_NAME")
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

func getPortNumber() string {
	return os.Getenv("HOLISTIC_DB_PORT_NUMBER")
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

func getDatabaseHostname() string {
	return os.Getenv("HOLISTIC_DB_HOSTNAME")
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

func getCredentials(label string) (string, string) {
	username := os.Getenv("HOLISTIC_DB_" + label + "_USERNAME")
	password := os.Getenv("HOLISTIC_DB_" + label + "_PASSWORD")
	return username, password
}

func validateCredentials(username string, password string) []error {
	var errors []error

	username_regex_exp := `^[A-Za-z]+$`
	username_regex_matcher, username_regex_errors := regexp.Compile(username_regex_exp)
	if username_regex_errors != nil {
		errors = append(errors, fmt.Errorf("username regex %s did not compile %s", username_regex_exp, username_regex_errors.Error()))
	}

	if !username_regex_matcher.MatchString(username) {
		errors = append(errors, fmt.Errorf("username %s did not match regex %s", username, username_regex_exp))
	}

	password_errors := validatePassword(password)
	if password_errors != nil {
		errors = append(errors, password_errors...)
	}

	return errors
}

func validatePassword(password string) []error {
	var uppercasePresent bool
	var lowercasePresent bool
	var numberPresent bool
	var specialCharPresent bool
	const minPassLength = 8
	var passLen int
	var errors []error

	for _, ch := range password {
		switch {
		case unicode.IsNumber(ch):
			numberPresent = true
			passLen++
		case unicode.IsUpper(ch):
			uppercasePresent = true
			passLen++
		case unicode.IsLower(ch):
			lowercasePresent = true
			passLen++
		case unicode.IsPunct(ch) || unicode.IsSymbol(ch):
			specialCharPresent = true
			passLen++
		}
	}

	if !lowercasePresent {
		errors = append(errors, fmt.Errorf("lowercase letter missing"))
	}
	if !uppercasePresent {
		errors = append(errors, fmt.Errorf("uppercase letter missing"))
	}
	if !numberPresent {
		errors = append(errors, fmt.Errorf("at least one numeric character required"))
	}
	if !specialCharPresent {
		errors = append(errors, fmt.Errorf("at least one special character required"))

	}
	if passLen <= minPassLength {
		errors = append(errors, fmt.Errorf("password length must be at least %d characters long", minPassLength))
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}