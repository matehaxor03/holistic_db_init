package db_installer

import (
	"fmt"
	"strconv"
	json "github.com/matehaxor03/holistic_json/json"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	common "github.com/matehaxor03/holistic_common/common"
	validation_constants "github.com/matehaxor03/holistic_validator/validation_constants"
	validate "github.com/matehaxor03/holistic_validator/validate"
	host_client "github.com/matehaxor03/holistic_host_client/host_client"
)

type DatabaseInstaller struct {
	Validate func() []error
	Install func() ([]error)
}

func NewDatabaseInstaller(database_host_name string, database_port_number string, database_name string, database_root_user string, database_root_password string, write_host_users []string, read_host_users []string, migration_host_users []string) (*DatabaseInstaller, []error) {
	verify := validate.NewValidator()
	db_host_name := database_host_name
	db_port_number := database_port_number
	db_name := database_name
	database_username := database_root_user
	database_password := database_root_password

	host_client_instance, host_client_errors := host_client.NewHostClient()
	if host_client_errors != nil {
		return nil, host_client_errors
	}
	
	getDatabaseHostName := func() string {
		return db_host_name
	}

	getDatabasePortNumber := func() string {
		return db_port_number
	}

	getDatabaseName := func() string {
		return db_name
	}

	getDatabaseRootUsername := func() string {
		return database_username
	}

	getDatabaseRootPassword := func() string {
		return database_password
	}

	writeCredentialsFile := func(host_usernames []string, host_name string, port_number string, database_name string, username string, password string, user_count int) []error {
		var errors []error

		user_count_as_string := ""
		if user_count != -1 {
			user_count_as_string = fmt.Sprintf("%d", user_count)
		}

		for _, host_username := range host_usernames {
			fmt.Println(host_username)
			host_user, host_user_errors := host_client_instance.User(host_username)
			if host_user_errors != nil {
				return host_user_errors
			}

			host_home_directory, host_home_directory_errors := host_user.GetHomeDirectoryAbsoluteDirectory()
			if host_home_directory_errors != nil {
				return host_home_directory_errors
			}	
			
			var db_creds_directory_path []string
			db_creds_directory_path = append(db_creds_directory_path, host_home_directory.GetPath()...)
			db_creds_directory_path = append(db_creds_directory_path, ".db")
			
			db_creds_directory, db_creds_directory_errors := host_client_instance.AbsoluteDirectory(db_creds_directory_path)
			if db_creds_directory_errors != nil {
				return db_creds_directory_errors
			}

			db_creds_directory_create_errors := db_creds_directory.CreateIfDoesNotExist()
			if db_creds_directory_create_errors != nil {
				return db_creds_directory_create_errors
			}

			db_creds_file, db_creds_file_errors := host_client_instance.AbsoluteFile(*db_creds_directory, "holistic_db_config#" + host_name  + "#" + port_number + "#" + database_name + "#"  + username + user_count_as_string + ".config")
			if db_creds_file_errors != nil {
				return db_creds_file_errors
			}

			remove_db_file_if_exists_errors := db_creds_file.RemoveIfExists()
			if remove_db_file_if_exists_errors != nil {
				return remove_db_file_if_exists_errors
			}

			create_file_errors := db_creds_file.Create()
			if create_file_errors != nil {
				return create_file_errors
			}

			db_creds_file_append_errors := db_creds_file.Append("[client]\n" + "user=" + (username + user_count_as_string) + "\npassword=" + password + "\n[mysqld]\nskip-log-bin")
			if db_creds_file_append_errors != nil {
				return db_creds_file_append_errors
			}

			user_primary_group, user_primary_group_errors := host_user.GetPrimaryGroup()
			if user_primary_group_errors != nil {
				return user_primary_group_errors
			} else if user_primary_group == nil {
				errors = append(errors, fmt.Errorf("primary group is nil"))
				return errors
			}

			set_owner_errors := db_creds_file.SetOwner(*host_user, *user_primary_group)
			if set_owner_errors != nil {
				return set_owner_errors
			}

			set_directory_owner_errors := db_creds_directory.SetOwner(*host_user, *user_primary_group)
			if set_directory_owner_errors != nil {
				return set_directory_owner_errors
			}
		}
		return nil
	}
	
	
	install := func() ([]error) {
		directory_parts := common.GetDataDirectory()
		directory := "/" 
		for index, directory_part := range directory_parts {
			directory += directory_part
			if index < len(directory_parts) - 1 {
				directory += "/"
			}
		}

		var errors []error
		db_hostname := getDatabaseHostName()
		db_port_number := getDatabasePortNumber()
		db_name := getDatabaseName()
		root_db_username := getDatabaseRootUsername()
		root_db_password := getDatabaseRootPassword()
		migration_db_username := common.CONSTANT_HOLISTIC_DATABASE_MIGRATION_USERNAME()
		migration_db_password := common.GenerateGuid()

		write_db_username := common.CONSTANT_HOLISTIC_DATABASE_WRITE_USERNAME()
		write_db_password := common.GenerateGuid()
	
		read_db_username := common.CONSTANT_HOLISTIC_DATABASE_READ_USERNAME()
		read_db_password := common.GenerateGuid()

		var all_host_users []string
		all_host_users = append(all_host_users, write_host_users...)
		all_host_users = append(all_host_users, read_host_users...)
		all_host_users = append(all_host_users, migration_host_users...)

		root_errors := writeCredentialsFile(all_host_users, db_hostname, db_port_number, "", root_db_username, root_db_password, -1)
		if root_errors != nil {
			return root_errors
		}

		root_errors2 := writeCredentialsFile(all_host_users, db_hostname, db_port_number, db_name, root_db_username, root_db_password, -1)
		if root_errors2 != nil {
			return root_errors2
		}

		root_errors3 := writeCredentialsFile(all_host_users, db_hostname, db_port_number, "mysql", root_db_username, root_db_password, -1)
		if root_errors3 != nil {
			return root_errors3
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

		if len(errors) > 0 {
			return errors
		}

		client_manager, client_manager_errors := dao.NewClientManager()
		if client_manager_errors != nil {
			errors = append(errors, client_manager_errors...)
		}

		if len(errors) > 0 {
			return errors
		}

		client, client_errors := client_manager.GetClient(db_hostname, db_port_number, db_name, root_db_username)
		if client_errors != nil {
			errors = append(errors, client_errors...)
		}

		if len(errors) > 0 {
			return errors
		}

		database_exists, database_exists_errors := client.DatabaseExists(db_name)
		if database_exists_errors != nil {
			return database_exists_errors
		}
		
		if !database_exists {
			character_set := validation_constants.GET_CHARACTER_SET_UTF8MB4()
			collate := validation_constants.GET_COLLATE_UTF8MB4_0900_AI_CI()

			fmt.Println("creating database...")
			_, database_creation_errs := client.CreateDatabase(db_name, &character_set, &collate)
			if database_creation_errs != nil {
				errors = append(errors, database_creation_errs...)		
				return errors
			}
		} else {
			fmt.Println("(skip) database already exists...")
		}

		database := client.GetDatabase()
		set_root_database_username_errors := database.SetDatabaseUsername(root_db_username)
		if set_root_database_username_errors != nil {
			return set_root_database_username_errors
		}

		use_database_errors := client.UseDatabase(*database)
		if use_database_errors != nil {
			fmt.Println("use database errors ...")
			return use_database_errors
		}

		disable_global_logs_errors := database.GlobalGeneralLogDisable()
		if disable_global_logs_errors != nil {
			return disable_global_logs_errors
		}

		set_utc_time_errors := database.GlobalSetTimeZoneUTC()
		if set_utc_time_errors != nil {
			return set_utc_time_errors
		}

		set_sql_mode_errors := database.GlobalSetSQLMode()
		if set_sql_mode_errors != nil {
			return set_sql_mode_errors
		}

		database_filter := db_name
		table_filter := "*"
		
		migration_user_exists, migration_user_exists_errors := client.UserExists(migration_db_username)
		if migration_user_exists_errors != nil {
			fmt.Println("migration user exists errors ...")
			return migration_user_exists_errors
		}

		if !migration_user_exists {
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

		migration_errors := writeCredentialsFile(migration_host_users, db_hostname, db_port_number, db_name, migration_db_username, migration_db_password, -1)
		if migration_errors != nil {
			return migration_errors
		}

		user_count := 0
		for user_count < 100 {
			fmt.Println(user_count)
			write_user_exists, write_user_exists_errors := client.UserExists(write_db_username + fmt.Sprintf("%d", user_count))
			if write_user_exists_errors != nil {
				return write_user_exists_errors
			}
			if !write_user_exists {
				fmt.Println("creating write database user...")
				write_db_user, create_write_user_errs := client.CreateUser(write_db_username + fmt.Sprintf("%d", user_count), write_db_password, db_hostname)
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
			write_db_user, write_db_user_errors := client.GetUser(write_db_username + fmt.Sprintf("%d", user_count))
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

			write_errors := writeCredentialsFile(write_host_users, db_hostname, db_port_number, db_name, write_db_username, write_db_password, user_count)
			if write_errors != nil {
				return write_errors
			}

			read_user_exists, read_user_exists_errors := client.UserExists(read_db_username + fmt.Sprintf("%d", user_count))
			if read_user_exists_errors != nil {
				return read_user_exists_errors
			}
			if !read_user_exists {
				fmt.Println("creating read database user...")
				read_db_user, create_read_user_errs := client.CreateUser(read_db_username + fmt.Sprintf("%d", user_count), read_db_password, db_hostname)
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
			read_db_user, read_db_user_errors := client.GetUser(read_db_username + fmt.Sprintf("%d", user_count))
			if read_db_user_errors != nil {
				return read_db_user_errors
			}

			fmt.Println("granting permissions to read database user...")
			_, grant_read_db_user_errors := client.Grant(*read_db_user, "SELECT", &database_filter, &table_filter)
			if grant_read_db_user_errors != nil {
				return grant_read_db_user_errors
			}
			
			read_errors := writeCredentialsFile(read_host_users, db_hostname, db_port_number, db_name, read_db_username, read_db_password, user_count)
			if read_errors != nil {
				return read_errors
			}
			
			user_count++
		}
		

		set_database_username_errors := client.SetDatabaseUsername(migration_db_username)
		if set_database_username_errors != nil {
			return set_database_username_errors
		}
		

		data_migration_table_exists, data_migration_table_exists_errors := database.TableExists("DatabaseMigration")
		if data_migration_table_exists_errors != nil {
			return data_migration_table_exists_errors
		}

		if !data_migration_table_exists {

			database_migration_schema := json.NewMapValue()

			primary_key_column_schema := json.NewMapValue()
			primary_key_column_schema.SetStringValue("type", "uint64")
			primary_key_column_schema.SetBoolValue("auto_increment", true)
			primary_key_column_schema.SetBoolValue("primary_key", true)

			current_column_schema := json.NewMapValue()
			current_column_schema.SetStringValue("type", "int64")
			current_column_schema.SetInt64Value("default", int64(-1))

			desired_column_schema := json.NewMapValue()
			desired_column_schema.SetStringValue("type", "int64")
			desired_column_schema.SetInt64Value("default", int64(0))

			database_migration_schema.SetMapValue("database_migration_id", primary_key_column_schema)
			database_migration_schema.SetMapValue("current", current_column_schema)
			database_migration_schema.SetMapValue("desired", desired_column_schema)


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
		
		data_migration_table_record_count, data_migration_table_record_count_errors := data_migration_table.Count(nil, nil, nil, nil, nil)
		if data_migration_table_record_count_errors != nil {
			return data_migration_table_record_count_errors
		}

		if *data_migration_table_record_count > 0 {
			fmt.Println("(skip) database migration record already exists...")
			return nil
		}

		fmt.Println("creating database migration record...")
		default_record := json.NewMapValue()
		inserted_record, inserted_record_errors := data_migration_table.CreateRecord(default_record)
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


	validate := func() []error {
		var errors []error
		temp_database_hostname := getDatabaseHostName()
		temp_database_port_number := getDatabasePortNumber()
		temp_database_name := getDatabaseName()
		temp_database_username := getDatabaseRootUsername()
		temp_database_password := getDatabaseRootPassword()

		database_host_name_errors := verify.ValidateDomainName(temp_database_hostname)
		if database_host_name_errors != nil {
			errors = append(errors, database_host_name_errors...)
		}

		database_port_number_errors := verify.ValidatePortNumber(temp_database_port_number)
		if database_port_number_errors != nil {
			errors = append(errors, database_port_number_errors...)
		}

		database_name_errors := verify.ValidateDatabaseName(temp_database_name)
		if database_name_errors != nil {
			errors = append(errors, database_name_errors...)
		}

		username_errors := verify.ValidateUsername(temp_database_username)
		if username_errors != nil {
			errors = append(errors, username_errors...)
		}

		password_errors := verify.ValidateBase64Encoding(temp_database_password)
		if password_errors != nil {
			errors = append(errors, password_errors...)
		}

		if errors != nil {
			return errors
		}

		return nil
	}

	x := DatabaseInstaller{
		Validate: func() []error {
			return validate()
		},
		Install: func() ([]error) {
			return install()
		},
	}

	errors := validate()

	if errors != nil {
		return nil, errors
	}

	return &x, nil
}

