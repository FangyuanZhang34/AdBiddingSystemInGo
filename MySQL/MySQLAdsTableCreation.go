package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	// connect to db
	db, err := sql.Open("mysql", "root:root@(localhost:3306)/AdSysGo")
	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}
	// defer the close
	defer db.Close()

	// Drop table users if exists
	stmt, err := db.Prepare("DROP TABLE IF EXISTS ad;")
	if err != nil {
		fmt.Println(err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("ad Table dropped successfully..")
	}

	stmt, err = db.Prepare("DROP TABLE IF EXISTS advertiser;")
	if err != nil {
		fmt.Println(err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("advertiser Tables dropped successfully..")
	}

	// create table advertiser
	stmt, err = db.Prepare("CREATE TABLE advertiser (advertiser_id INT NOT NULL AUTO_INCREMENT, name VARCHAR(255), budget FLOAT, PRIMARY KEY (advertiser_id));")
	if err != nil {
		fmt.Println(err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("advertiser Table created successfully..")
	}
	defer stmt.Close()

	// create table ad
	stmt, err = db.Prepare("CREATE TABLE ad (ad_id INT NOT NULL AUTO_INCREMENT, bid FLOAT, image_url VARCHAR(2083), advertiser_id INT NOT NULL, ad_score FLOAT, PRIMARY KEY(ad_id), FOREIGN KEY(advertiser_id) REFERENCES advertiser(advertiser_id));")
	if err != nil {
		fmt.Println(err.Error())
	}
	_, err = stmt.Exec()
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("ad Table created successfully..")
	}
	defer stmt.Close()

	// perform a db.Query insert
	insert, err := db.Query("INSERT INTO advertiser (name, budget) VALUES('Fangyuan', 10000);")
	insert, err = db.Query("INSERT INTO ad (bid, advertiser_id, ad_score) VALUES(10, 1, 20)")

	// if there is an error inserting, handle it
	if err != nil {
		panic(err.Error())
	}
	// be careful deferring Queries if you are using transactions
	defer insert.Close()

}
