package main

import (
	"fmt"
	"os"
	db_installer "github.com/matehaxor03/holistic_db_init/db_installer"
)

func main() {
	database_installer,  database_installer_errors := db_installer.NewDatabaseInstaller()
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