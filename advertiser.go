package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

/*
return:
	ad exists: true, nil
	ad not exists: false, nil
	other error: false, err
*/
func checkAdvertiserExists(db *sql.DB, advertiser Advertiser) (bool, error) {
	var name string
	err := db.QueryRow("SELECT name FROM advertiser WHERE name = ?", advertiser.Name).Scan(&name)
	// fail to select
	if err != nil && err != sql.ErrNoRows {
		return false, errors.New("Failed to SELECT from advertiser table")
	}
	// not exist
	if err == sql.ErrNoRows {
		return false, nil
	}
	// exist
	return true, nil
}

/*
use "checkAdvertiserExists" to check if the advertiser already exists
then, insert an advertiser into advertiser table
return:
	err
	nil
*/
func insertAdvertiser(advertiser Advertiser) error {
	db, err := sql.Open("mysql", mysqlDataSourceName)
	if err != nil {
		return errors.New("Failed to connect to MySQL database")
	}
	defer db.Close()

	// check if the advertiser already exists
	exists, err := checkAdvertiserExists(db, advertiser)
	if err != nil {
		return errors.New("Failed to select from advertiser table")
	}
	if exists {
		return errors.New("Advertiser already exists")
	}

	// if not exist, insert the advertiser into advertiser table
	insert, err := db.Query("INSERT INTO advertiser (name, budget) VALUES(?, ?)", advertiser.Name, advertiser.Budget)
	if err != nil {
		return errors.New("Failed to insert into advertiser table")
	}
	defer insert.Close()

	return nil
}

/*
HanldeFunction
use "insertAdvertiser" to add a row of an advertiser into advertiser table
*/
func handleFuncAddAdvertiser(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one advertiser insertion request")
	w.Header().Set("Content-Type", "text/plain")

	if req.Method != "POST" {
		return
	}

	// decode the json format info into Advertiser type
	decoder := json.NewDecoder(req.Body)
	var advertiser Advertiser
	if err := decoder.Decode(&advertiser); err != nil {
		http.Error(w, "Cannot decode advertiser's data from client", 400)
		fmt.Println("Cannot decode advertiser's data from client.", err)
		return
	}
	// insert advertiser into advertiser table
	if err := insertAdvertiser(advertiser); err != nil {
		if err.Error() == "Failed to connect MySQL database" {
			http.Error(w, "Failed to connect MySQL database.", 500)
		} else if err.Error() == "Failed to select from advertiser table" {
			http.Error(w, "Failed to select from advertiser table.", 500)
		} else if err.Error() == "Advertiser already exists" {
			http.Error(w, "Advertiser already exists.", 400)
		} else if err.Error() == "Failed to insert into advertiser table" {
			http.Error(w, "Failed to insert into advertiser table.", 500)
		}
		return
	}
	w.Write([]byte("Advertiser added successfully"))

}
func addBudget(process AddBudgetProcess) error {
	// connect the database
	db, err := sql.Open("mysql", mysqlDataSourceName)
	if err != nil {
		return errors.New("Failed to connect the database")
	}
	defer db.Close()

	// select advertiser with advertiserID
	sele, err := db.Query("SELECT budget FROM advertiser WHERE advertiser_id=(?)", process.AdvertiserID)
	if err != nil {
		return errors.New("Failed to select from advertiser table")
	}
	defer sele.Close()

	// get old budget from selected result
	var newBudget float64
	for sele.Next() {
		var nilBudget sql.NullFloat64
		if err = sele.Scan(&nilBudget); err != nil {
			return errors.New("Failed to get old budget")
		}
		newBudget = nilBudget.Float64 + process.AddBudget
	}

	// update old budget with new budget
	update, err := db.Query("UPDATE advertiser SET budget=(?) WHERE advertiser_id=(?)", newBudget, process.AdvertiserID)
	if err != nil {
		return errors.New("Failed to update budget")
	}
	defer update.Close()

	return nil
}

func handleFuncAddBudget(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one add budget request")
	w.Header().Set("Content-Type", "text/plain")

	if req.Method != "POST" {
		return
	}

	// decode the json format info into AddBudget type
	decoder := json.NewDecoder(req.Body)
	var addBudgetProcess AddBudgetProcess
	if err := decoder.Decode(&addBudgetProcess); err != nil {
		http.Error(w, "Cannot decode addBudgetProcess data from client", 400)
		fmt.Println("Cannot decode addBudgetProcess data from client.", err)
		return
	}
	// add budget
	if err := addBudget(addBudgetProcess); err != nil {
		if err.Error() == "Failed to connect the database" {
			http.Error(w, "Failed to connect the database.", 500)
		} else if err.Error() == "Failed to select from advertiser table" {
			http.Error(w, "Failed to select from advertiser table.", 500)
		} else if err.Error() == "Failed to get old budget" {
			http.Error(w, "Failed to get old budget.", 500)
		} else if err.Error() == "Failed to update budget" {
			http.Error(w, "Failed to update budget.", 500)
		}
	}
	w.Write([]byte("Budget added successfully"))
}

func searchAdvertiser(searchName string) (Advertiser, error) {
	var advertiser Advertiser
	// connect to database
	db, err := sql.Open("mysql", mysqlDataSourceName)
	if err != nil {
		return advertiser, errors.New("Failed to connect the database")
	}
	defer db.Close()

	// select a row of advertiser infomation from table by name
	var id int
	var name string
	var budget sql.NullFloat64
	if err = db.QueryRow("SELECT advertiser_id, name, budget FROM advertiser WHERE name = ?", searchName).Scan(&id, &name, &budget); err != nil {
		return advertiser, errors.New("Failed to select from advertiser table")
	}
	// convert to Advertiser type
	advertiser.AdvertiserID, advertiser.Name, advertiser.Budget = id, name, budget.Float64
	return advertiser, nil
}

func handleFuncSearchAdvertiser(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Received one search advertiser request")
	w.Header().Set("Content-Type", "text/plain")

	if req.Method != "POST" {
		return
	}

	// decode json from request
	decoder := json.NewDecoder(req.Body)
	var advertiser Advertiser
	if err := decoder.Decode(&advertiser); err != nil {
		http.Error(w, "Cannot decode searchAdvertiserProcess data from client", 400)
		fmt.Println("Cannot decode searchAdvertiserProcess data from client.", err)
		return
	}

	// search advertiser by name
	advertiser, err := searchAdvertiser(advertiser.Name)
	if err != nil {
		if err.Error() == "Failed to connect the database" {
			http.Error(w, "Failed to connect the database.", 500)
		} else if err.Error() == "Failed to select from advertiser table" {
			http.Error(w, "Failed to select from advertiser table.", 500)
		}
		return
	}

	// encode the advertiser info as json format
	advertiserInfo, err := json.Marshal(advertiser)
	if err != nil {
		http.Error(w, "Failed to parse advertiser data into JSON format", 500)
		fmt.Printf("Failed to parse advertiser data into JSON format %v.\n", err)
		return
	}

	w.Write(advertiserInfo)

}
