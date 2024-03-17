package main

import (
	"fmt"
	"os"
	"strings"
	host_client "github.com/matehaxor03/holistic_host_client/host_client"
	common "github.com/matehaxor03/holistic_common/common"
	db_installer "github.com/matehaxor03/holistic_db_init/db_installer"
)

func main() {
	var errors []error
	host_client, host_client_errors := host_client.NewHostClient()
	if host_client_errors != nil {
		fmt.Println(fmt.Errorf("%s", host_client_errors))
		os.Exit(1)
	}

	database_host_name, database_host_name_errors := host_client.GetEnviornmentVariable(common.ENV_HOLISTIC_DATABASE_HOSTNAME())
	if database_host_name_errors != nil {
		errors = append(errors, database_host_name_errors...)
	}

	database_port_number, database_port_number_errors := host_client.GetEnviornmentVariable(common.ENV_HOLISTIC_DATABASE_PORT_NUMBER())
	if database_port_number_errors != nil {
		errors = append(errors, database_port_number_errors...)
	}

	database_name, database_name_errors := host_client.GetEnviornmentVariable(common.ENV_HOLISTIC_DATABASE_NAME())
	if database_name_errors != nil {
		errors = append(errors, database_name_errors...)
	}

	database_root_username, database_root_username_errors := host_client.GetEnviornmentVariable(common.ENV_HOLISTIC_DATABASE_ROOT_USERNAME())
	if database_root_username_errors != nil {
		errors = append(errors, database_root_username_errors...)
	}

	database_root_password, database_root_password_errors := host_client.GetEnviornmentVariable(common.ENV_HOLISTIC_DATABASE_ROOT_PASSWORD())
	if database_root_password_errors != nil {
		errors = append(errors, database_root_password_errors...)
	}


	writer_raw_host_usernames, writer_raw_host_usernames_errors := host_client.GetEnviornmentVariable(common.ENV_HOLISTIC_DATABASE_WRITER_USERNAMES())
	if writer_raw_host_usernames_errors != nil {
		errors = append(errors, writer_raw_host_usernames_errors...)
	}

	reader_raw_host_usernames, reader_raw_host_usernames_errors := host_client.GetEnviornmentVariable(common.ENV_HOLISTIC_DATABASE_READER_USERNAMES())
	if reader_raw_host_usernames_errors != nil {
		errors = append(errors, reader_raw_host_usernames_errors...)
	}

	migration_raw_host_usernames, migration_raw_host_usernames_errors := host_client.GetEnviornmentVariable(common.ENV_HOLISTIC_DATABASE_MIGRATION_USERNAMES())
	if migration_raw_host_usernames_errors != nil {
		errors = append(errors, migration_raw_host_usernames_errors...)
	}

	if len(errors) > 0 {
		fmt.Println(fmt.Errorf("%s", errors))
		os.Exit(1)
	}

	var writer_host_usernames []string
	var reader_host_usernames []string
	var migration_host_usernames []string

	{
		if strings.Contains(*writer_raw_host_usernames, ",") {
			temp := strings.Split(*writer_raw_host_usernames, ",")
			writer_host_usernames = append(writer_host_usernames, temp...)
		} else {
			writer_host_usernames = append(writer_host_usernames, *writer_raw_host_usernames)
		}
	}

	{
		if strings.Contains(*reader_raw_host_usernames, ",") {
			temp := strings.Split(*reader_raw_host_usernames, ",")
			reader_host_usernames = append(reader_host_usernames, temp...)
		} else {
			reader_host_usernames = append(reader_host_usernames, *reader_raw_host_usernames)
		}
	}


	{
		if strings.Contains(*migration_raw_host_usernames, ",") {
			temp := strings.Split(*migration_raw_host_usernames, ",")
			migration_host_usernames = append(migration_host_usernames, temp...)
		} else {
			migration_host_usernames = append(migration_host_usernames, *migration_raw_host_usernames)
		}
	}

	if len(errors) > 0 {
		fmt.Println(fmt.Errorf("%s", errors))
		os.Exit(1)
	}

	database_installer,  database_installer_errors := db_installer.NewDatabaseInstaller(*database_host_name, *database_port_number, *database_name, *database_root_username, *database_root_password, writer_host_usernames, reader_host_usernames, migration_host_usernames)
	if database_installer_errors != nil {
		fmt.Println(fmt.Errorf("%s", database_installer_errors))
		os.Exit(1)
	}

	install_errors := database_installer.Install()
	if install_errors != nil {
		fmt.Println(fmt.Errorf("%s", install_errors))
		os.Exit(1)
	}

	os.Exit(0)
}